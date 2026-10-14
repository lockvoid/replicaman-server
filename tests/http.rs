use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    http::{HeaderMap, Request, StatusCode, header},
    response::Response,
};
use http_body_util::{BodyExt, Full};
use replicaman_server::{
    Error, ErrorCode, Result,
    backend::{Backend, Context},
    engine::{Outcome, PullResponse, PushResponse, SnapshotResponse},
    http::{AuthIdentity, Authenticator, HttpConfig, router, router_with_config},
    wire::{MAX_BODY_BYTES, Position, PullRequest, PushOperation, PushRequest, SnapshotRequest},
};
use serde_json::{Value, json};
use std::{
    convert::Infallible,
    future::pending,
    io,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Notify;
use tower::ServiceExt;
use uuid::Uuid;

const EPOCH: &str = "f4c05d55-70c7-41d9-9868-c0582ae3a020";
const OP: &str = "13fb2e27-3bf1-4415-b5a4-904e37b6494c";
const PULL: &str =
    r#"{"stream":"catalog","epoch":"f4c05d55-70c7-41d9-9868-c0582ae3a020","cursor":"0"}"#;

#[derive(Clone)]
struct TestAuth {
    result: Result<AuthIdentity>,
    delay: Duration,
    hang: bool,
}

impl Default for TestAuth {
    fn default() -> Self {
        Self {
            result: Ok("stable-account".into()),
            delay: Duration::ZERO,
            hang: false,
        }
    }
}

impl Authenticator for TestAuth {
    async fn authenticate(&self, _: &HeaderMap) -> Result<AuthIdentity> {
        if self.hang {
            pending::<()>().await;
        }
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.result.clone()
    }
}

#[derive(Clone, Debug)]
struct Call {
    method: &'static str,
    context: Context,
    at: Instant,
}

#[derive(Clone, Default)]
struct StubBackend {
    calls: Arc<Mutex<Vec<Call>>>,
    entered: Arc<Notify>,
    failure: Option<Error>,
    hang: bool,
    oversized: bool,
}

impl StubBackend {
    async fn before(&self, method: &'static str, context: Context) -> Result<()> {
        self.calls.lock().unwrap().push(Call {
            method,
            context,
            at: Instant::now(),
        });
        self.entered.notify_one();
        if self.hang {
            pending::<()>().await;
        }
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        Ok(())
    }
}

impl Backend for StubBackend {
    async fn push(&self, context: Context, request: PushRequest) -> Result<PushResponse> {
        self.before("push", context).await?;
        let (op, reason) = match request.operation {
            PushOperation::Message(message) => (message.id(), "domain_refused"),
            PushOperation::Cancel { op, .. } => (op, "cancelled"),
        };
        Ok(PushResponse {
            stream: request.stream,
            epoch: request.epoch,
            op,
            outcome: Outcome::Rejected {
                reason: reason.into(),
            },
        })
    }

    async fn pull(&self, context: Context, request: PullRequest) -> Result<PullResponse> {
        self.before("pull", context).await?;
        let to = request.to.unwrap_or(request.cursor);
        Ok(PullResponse {
            stream: if self.oversized {
                "x".repeat(MAX_BODY_BYTES)
            } else {
                request.stream
            },
            epoch: request.epoch,
            events: vec![],
            cursor: to,
            to,
            head: to,
            more: false,
        })
    }

    async fn snapshot(
        &self,
        context: Context,
        request: SnapshotRequest,
    ) -> Result<SnapshotResponse> {
        self.before("snapshot", context).await?;
        Ok(SnapshotResponse {
            stream: request.stream,
            epoch: request.epoch,
            client: request.client,
            attempt: request.attempt,
            snapshot: Uuid::new_v4(),
            cursor: Position(0),
            items: vec![],
            next: None,
            ttl: 600,
            ops: vec![],
        })
    }
}

fn request(path: &str, body: impl Into<Body>) -> Request<Body> {
    Request::post(format!("/replicaman/v1/{path}"))
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer changing-token")
        .body(body.into())
        .unwrap()
}

fn push_body() -> String {
    json!({
        "stream": "catalog", "epoch": EPOCH,
        "message": {
            "specversion": "1.0", "id": OP, "source": "urn:forged:identity",
            "type": "test.v1", "datacontenttype": "application/json", "data_base64": "e30="
        }
    })
    .to_string()
}

fn assert_headers(response: &Response) {
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
}

async fn body_json(response: Response) -> Value {
    assert_headers(&response);
    let bytes = to_bytes(response.into_body(), MAX_BODY_BYTES)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn assert_error(response: Response, status: StatusCode, code: &str) {
    assert_eq!(response.status(), status);
    assert_eq!(body_json(response).await["error"], code);
}

fn short_router(backend: StubBackend, auth: TestAuth) -> Router {
    router_with_config(
        backend,
        auth,
        HttpConfig {
            exchange_timeout: Duration::from_millis(30),
            max_in_flight: 1,
            ..HttpConfig::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn routes_dispatch_and_rejected_receipts_are_http_200() {
    let backend = StubBackend::default();
    let app = router(backend.clone(), TestAuth::default());
    let response = app
        .clone()
        .oneshot(request("push", push_body()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        body_json(response).await,
        json!({
            "stream": "catalog", "epoch": EPOCH, "op": OP,
            "status": "rejected", "reason": "domain_refused"
        })
    );

    let response = app.clone().oneshot(request("pull", PULL)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value = body_json(response).await;
    assert_eq!(value["cursor"], "0");
    assert_eq!(value["more"], false);

    let response = app
        .oneshot(request(
            "snapshot",
            json!({
                "stream": "catalog", "epoch": EPOCH,
                "client": OP, "attempt": Uuid::new_v4(), "page": null
            })
            .to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_json(response).await["ttl"], 600);

    let calls = backend.calls.lock().unwrap();
    assert_eq!(
        calls.iter().map(|call| call.method).collect::<Vec<_>>(),
        ["push", "pull", "snapshot"]
    );
    assert!(
        calls
            .iter()
            .all(|call| call.context.subject == "stable-account")
    );
}

#[tokio::test]
async fn authentication_failure_and_empty_subject_never_reach_owner() {
    for result in [
        Err(Error::new(ErrorCode::Unauthorized)),
        Ok(String::new().into()),
    ] {
        let backend = StubBackend::default();
        let app = router(
            backend.clone(),
            TestAuth {
                result,
                ..TestAuth::default()
            },
        );
        let response = app.oneshot(request("push", push_body())).await.unwrap();
        assert_error(response, StatusCode::UNAUTHORIZED, "unauthorized").await;
        assert!(backend.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn malformed_and_duplicate_json_never_reach_owner() {
    let backend = StubBackend::default();
    let app = router(backend.clone(), TestAuth::default());
    let duplicate =
        format!(r#"{{"stream":"catalog","str\u0065am":"other","epoch":"{EPOCH}","cursor":"0"}}"#);
    for body in [duplicate.as_bytes(), b"{", &[0xff], b"{\"cursor\":NaN}"] {
        let response = app
            .clone()
            .oneshot(request("pull", body.to_vec()))
            .await
            .unwrap();
        assert_error(response, StatusCode::BAD_REQUEST, "bad_request").await;
    }
    assert!(backend.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn json_utf8_and_identity_are_supported() {
    let backend = StubBackend::default();
    let mut req = request("pull", PULL);
    req.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/json; charset=\"UTF-8\"".parse().unwrap(),
    );
    req.headers_mut()
        .insert(header::CONTENT_ENCODING, "identity".parse().unwrap());
    let response = router(backend, TestAuth::default())
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_headers(&response);
}

#[tokio::test]
async fn unsupported_media_and_encoding_are_415() {
    let backend = StubBackend::default();
    let app = router(backend.clone(), TestAuth::default());
    for (content_type, encoding) in [
        (None, None),
        (Some("text/plain"), None),
        (Some("application/problem+json"), None),
        (Some("application/json+custom"), None),
        (Some("application/json; charset=utf-16"), None),
        (Some("application/json"), Some("gzip")),
        (Some("application/json"), Some("identity, gzip")),
    ] {
        let mut req = request("pull", PULL);
        req.headers_mut().remove(header::CONTENT_TYPE);
        if let Some(value) = content_type {
            req.headers_mut()
                .insert(header::CONTENT_TYPE, value.parse().unwrap());
        }
        if let Some(value) = encoding {
            req.headers_mut()
                .insert(header::CONTENT_ENCODING, value.parse().unwrap());
        }
        let response = app.clone().oneshot(req).await.unwrap();
        assert_error(
            response,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
        )
        .await;
    }
    let mut duplicate = request("pull", PULL);
    duplicate
        .headers_mut()
        .append(header::CONTENT_TYPE, "text/plain".parse().unwrap());
    assert_error(
        app.oneshot(duplicate).await.unwrap(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_media_type",
    )
    .await;
    assert!(backend.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn body_limit_is_exact_and_does_not_depend_on_content_length() {
    let backend = StubBackend::default();
    let app = router(backend.clone(), TestAuth::default());
    for unknown_length in [false, true] {
        let data = vec![b' '; MAX_BODY_BYTES + 1];
        let body = if unknown_length {
            // map_frame has no size hint: the collector must count real bytes.
            Body::new(Full::new(Bytes::from(data)).map_frame(|frame| frame))
        } else {
            Body::from(data)
        };
        let response = app.clone().oneshot(request("pull", body)).await.unwrap();
        assert_error(response, StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large").await;
    }
    assert!(backend.calls.lock().unwrap().is_empty());
    let mut exact = PULL.as_bytes().to_vec();
    exact.resize(MAX_BODY_BYTES, b' ');
    let response = app.oneshot(request("pull", exact)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_headers(&response);
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn incomplete_body_is_not_misreported_as_a_size_error() {
    let backend = StubBackend::default();
    let body = Full::new(Bytes::from_static(PULL.as_bytes()))
        .map_err(|never| -> io::Error { match never {} })
        .with_trailers(async { Some(Err(io::Error::other("broken test body"))) });
    let response = router(backend.clone(), TestAuth::default())
        .oneshot(request("pull", Body::new(body)))
        .await
        .unwrap();
    assert_error(response, StatusCode::BAD_REQUEST, "bad_request").await;
    assert!(backend.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn fallback_and_wrong_method_use_protocol_headers_and_json() {
    let app = router(StubBackend::default(), TestAuth::default());
    for (method, path, status) in [
        ("POST", "/missing", StatusCode::NOT_FOUND),
        ("GET", "/replicaman/v1/push", StatusCode::METHOD_NOT_ALLOWED),
        ("PUT", "/replicaman/v1/pull", StatusCode::METHOD_NOT_ALLOWED),
        (
            "DELETE",
            "/replicaman/v1/snapshot",
            StatusCode::METHOD_NOT_ALLOWED,
        ),
    ] {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .unwrap();
        assert_error(
            app.clone().oneshot(req).await.unwrap(),
            status,
            "bad_request",
        )
        .await;
    }
}

#[tokio::test]
async fn error_status_epoch_and_retry_after_are_preserved() {
    for error in [
        Error::epoch_changed(Uuid::parse_str(EPOCH).unwrap()),
        Error::retry(ErrorCode::RateLimited, 17),
        Error::retry(ErrorCode::StoragePressure, 23),
        Error::retry(ErrorCode::SnapshotBusy, 1),
    ] {
        let app = router(
            StubBackend {
                failure: Some(error.clone()),
                ..StubBackend::default()
            },
            TestAuth::default(),
        );
        let response = app.oneshot(request("pull", PULL)).await.unwrap();
        assert_eq!(response.status().as_u16(), error.status());
        assert_eq!(response.extensions().get::<ErrorCode>(), Some(&error.error));
        if let Some(retry) = error.retry_after {
            assert_eq!(response.headers()[header::RETRY_AFTER], retry.to_string());
        }
        assert_eq!(
            body_json(response).await,
            serde_json::to_value(error).unwrap()
        );
    }
}

#[tokio::test]
async fn exchange_deadline_covers_authentication_and_body_collection() {
    let backend = StubBackend::default();
    let app = short_router(
        backend.clone(),
        TestAuth {
            hang: true,
            ..TestAuth::default()
        },
    );
    let response = tokio::time::timeout(Duration::from_secs(1), app.oneshot(request("pull", PULL)))
        .await
        .unwrap()
        .unwrap();
    assert_error(
        response,
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
    )
    .await;

    let stalled = Full::new(Bytes::from_static(PULL.as_bytes()))
        .with_trailers(pending::<Option<std::result::Result<HeaderMap, Infallible>>>());
    let app = short_router(backend.clone(), TestAuth::default());
    let response = tokio::time::timeout(
        Duration::from_secs(1),
        app.oneshot(request("pull", Body::new(stalled))),
    )
    .await
    .unwrap()
    .unwrap();
    assert_error(
        response,
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
    )
    .await;
    assert!(backend.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn owner_deadline_is_capped_by_transaction_and_remaining_exchange() {
    for transaction_timeout in [Duration::from_millis(80), Duration::from_secs(30)] {
        let backend = StubBackend::default();
        let exchange_timeout = Duration::from_secs(2);
        let app = router_with_config(
            backend.clone(),
            TestAuth {
                delay: Duration::from_millis(20),
                ..TestAuth::default()
            },
            HttpConfig {
                exchange_timeout,
                transaction_timeout,
                ..HttpConfig::default()
            },
        )
        .unwrap();
        let start = Instant::now();
        let response = app.oneshot(request("pull", PULL)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let calls = backend.calls.lock().unwrap();
        let call = &calls[0];
        assert!(call.context.deadline > call.at);
        assert!(call.context.deadline - call.at <= transaction_timeout.min(exchange_timeout));
        assert!(call.context.deadline <= start + exchange_timeout + Duration::from_millis(10));
    }
}

#[tokio::test]
async fn transaction_timeout_does_not_wait_for_the_larger_exchange_budget() {
    let backend = StubBackend {
        hang: true,
        ..StubBackend::default()
    };
    let app = router_with_config(
        backend.clone(),
        TestAuth::default(),
        HttpConfig {
            exchange_timeout: Duration::from_secs(2),
            transaction_timeout: Duration::from_millis(30),
            ..HttpConfig::default()
        },
    )
    .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(1), app.oneshot(request("pull", PULL)))
        .await
        .unwrap()
        .unwrap();
    assert_error(
        response,
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
    )
    .await;
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn overload_is_temporary_and_timeout_releases_admission() {
    let backend = StubBackend {
        hang: true,
        ..StubBackend::default()
    };
    let app = router_with_config(
        backend.clone(),
        TestAuth::default(),
        HttpConfig {
            exchange_timeout: Duration::from_millis(150),
            max_in_flight: 1,
            ..HttpConfig::default()
        },
    )
    .unwrap();
    let first = tokio::spawn(app.clone().oneshot(request("pull", PULL)));
    tokio::time::timeout(Duration::from_secs(1), backend.entered.notified())
        .await
        .unwrap();
    let response = app.clone().oneshot(request("pull", PULL)).await.unwrap();
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    assert_error(response, StatusCode::TOO_MANY_REQUESTS, "rate_limited").await;
    assert_error(
        first.await.unwrap().unwrap(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
    )
    .await;
    let response = app.oneshot(request("pull", PULL)).await.unwrap();
    assert_error(
        response,
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
    )
    .await;
    assert_eq!(backend.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn oversized_backend_output_cannot_escape_as_a_successful_response() {
    let app = router(
        StubBackend {
            oversized: true,
            ..StubBackend::default()
        },
        TestAuth::default(),
    );
    let response = app.oneshot(request("pull", PULL)).await.unwrap();
    assert_error(
        response,
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
    )
    .await;
}

#[test]
fn invalid_configuration_is_rejected_before_router_construction() {
    for config in [
        HttpConfig {
            exchange_timeout: Duration::ZERO,
            ..HttpConfig::default()
        },
        HttpConfig {
            transaction_timeout: Duration::ZERO,
            ..HttpConfig::default()
        },
        HttpConfig {
            max_in_flight: 0,
            ..HttpConfig::default()
        },
        HttpConfig {
            max_in_flight: usize::MAX,
            ..HttpConfig::default()
        },
        HttpConfig {
            exchange_timeout: Duration::MAX,
            ..HttpConfig::default()
        },
    ] {
        assert!(router_with_config(StubBackend::default(), TestAuth::default(), config).is_err());
    }
}
