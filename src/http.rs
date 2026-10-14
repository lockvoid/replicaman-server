//! Embeddable HTTP transport for the three Replicaman endpoints.
//!
//! Serve this router behind HTTPS. Authentication supplies a stable subject;
//! neither a CloudEvents source nor an installation's client ID is an identity.
//!
//! The exchange timeout includes authentication, body collection, parsing,
//! backend work, and serialization. The data owner must separately enforce
//! `Context::deadline`: dropping an HTTP future does not roll back a transaction
//! or establish that an operation was rejected. Retry its original operation ID.

use crate::{
    Error, ErrorCode, Result,
    backend::{Backend, Context},
    wire::{self, MAX_BODY_BYTES},
};
use axum::{
    Router,
    body::{Body, HttpBody},
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    middleware,
    response::Response,
    routing::{MethodFilter, on},
};
use serde::Serialize;
use std::{
    future::{Future, poll_fn},
    io::{self, Write},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

/// Method/path declarations used by registration and embedding applications.
pub const HTTP_ROUTES: [(Method, &str); 3] = [
    (Method::POST, "/replicaman/v1/push"),
    (Method::POST, "/replicaman/v1/pull"),
    (Method::POST, "/replicaman/v1/snapshot"),
];
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Host authentication, returning a stable nonempty authorization subject.
/// Token refresh must not change the subject for the same principal.
pub use crate::backend::AuthIdentity;

pub trait Authenticator: Send + Sync + 'static {
    fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> impl Future<Output = Result<AuthIdentity>> + Send;
}

#[derive(Clone, Debug)]
pub struct HttpConfig {
    /// Entire request, including a slow body or authentication service.
    pub exchange_timeout: Duration,
    /// Maximum budget passed to the data owner, capped by the exchange deadline.
    pub transaction_timeout: Duration,
    /// Requests admitted at once; excess requests receive 429 with Retry-After.
    pub max_in_flight: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            exchange_timeout: Duration::from_secs(60),
            transaction_timeout: Duration::from_secs(30),
            max_in_flight: 16,
        }
    }
}

impl HttpConfig {
    /// Invalid host configuration is reported before constructing the router.
    pub fn validate(&self) -> Result<()> {
        let now = Instant::now();
        if self.exchange_timeout.is_zero()
            || self.transaction_timeout.is_zero()
            || now.checked_add(self.exchange_timeout).is_none()
            || now.checked_add(self.transaction_timeout).is_none()
            || self.max_in_flight == 0
            || self.max_in_flight > Semaphore::MAX_PERMITS
        {
            return Err(Error::bad_request());
        }
        Ok(())
    }
}

struct App<B, A> {
    backend: B,
    auth: A,
    config: HttpConfig,
    permits: Arc<Semaphore>,
}

/// Build a router using 60-second exchanges and 30-second owner transactions.
pub fn router<B: Backend, A: Authenticator>(backend: B, auth: A) -> Router {
    router_with_config(backend, auth, HttpConfig::default())
        .expect("the default HTTP configuration is valid")
}

/// The 48 MiB protocol body limit is fixed, independent of host concurrency and
/// deadline configuration. Backends also enforce the smaller page/item limits.
pub fn router_with_config<B: Backend, A: Authenticator>(
    backend: B,
    auth: A,
    config: HttpConfig,
) -> Result<Router> {
    config.validate()?;
    let state = Arc::new(App {
        backend,
        auth,
        permits: Arc::new(Semaphore::new(config.max_in_flight)),
        config,
    });
    let [
        (push_method, push_path),
        (pull_method, pull_path),
        (snapshot_method, snapshot_path),
    ] = HTTP_ROUTES.map(|(method, path)| {
        (
            MethodFilter::try_from(method).expect("standard HTTP method"),
            path,
        )
    });
    Ok(Router::new()
        .route(push_path, on(push_method, push::<B, A>))
        .route(pull_path, on(pull_method, pull::<B, A>))
        .route(snapshot_path, on(snapshot_method, snapshot::<B, A>))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(middleware::map_response(protocol_headers))
        .with_state(state))
}

#[derive(Clone, Copy)]
enum Endpoint {
    Push,
    Pull,
    Snapshot,
}

async fn push<B: Backend, A: Authenticator>(
    State(state): State<Arc<App<B, A>>>,
    request: Request,
) -> Response {
    exchange(state, request, Endpoint::Push).await
}

async fn pull<B: Backend, A: Authenticator>(
    State(state): State<Arc<App<B, A>>>,
    request: Request,
) -> Response {
    exchange(state, request, Endpoint::Pull).await
}

async fn snapshot<B: Backend, A: Authenticator>(
    State(state): State<Arc<App<B, A>>>,
    request: Request,
) -> Response {
    exchange(state, request, Endpoint::Snapshot).await
}

async fn exchange<B: Backend, A: Authenticator>(
    state: Arc<App<B, A>>,
    request: Request,
    endpoint: Endpoint,
) -> Response {
    let Some(deadline) = Instant::now().checked_add(state.config.exchange_timeout) else {
        return error_response(Error::internal());
    };
    let permit = match Arc::clone(&state.permits).try_acquire_owned() {
        Ok(permit) => Arc::new(permit),
        Err(_) => return error_response(Error::retry(ErrorCode::RateLimited, 1)),
    };
    let result = tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        execute(&state, request, endpoint, deadline, permit),
    )
    .await;
    // A ready future can win a timeout poll at its deadline. Never report a
    // late result as a timely response, even if serialization just completed.
    if Instant::now() >= deadline {
        return error_response(Error::internal());
    }
    match result {
        Ok(Ok(body)) => Response::new(Body::from(body)),
        Ok(Err(error)) => error_response(error),
        Err(_) => error_response(Error::internal()),
    }
}

async fn execute<B: Backend, A: Authenticator>(
    state: &App<B, A>,
    request: Request,
    endpoint: Endpoint,
    deadline: Instant,
    permit: Arc<OwnedSemaphorePermit>,
) -> Result<Vec<u8>> {
    validate_headers(request.headers())?;
    let identity = state.auth.authenticate(request.headers()).await?;
    if identity.subject.is_empty() {
        return Err(Error::new(ErrorCode::Unauthorized));
    }
    let body = collect_body(request.into_body()).await?;
    match endpoint {
        Endpoint::Push => {
            let request = parse_body(body, wire::parse_push, Arc::clone(&permit)).await?;
            let context = owner_context(identity, &state.config, deadline)?;
            let result =
                owner_result(context.deadline, state.backend.push(context, request)).await?;
            serialize_response(result, permit).await
        }
        Endpoint::Pull => {
            let request = parse_body(body, wire::parse_pull, Arc::clone(&permit)).await?;
            let context = owner_context(identity, &state.config, deadline)?;
            let result =
                owner_result(context.deadline, state.backend.pull(context, request)).await?;
            serialize_response(result, permit).await
        }
        Endpoint::Snapshot => {
            let request = parse_body(body, wire::parse_snapshot, Arc::clone(&permit)).await?;
            let context = owner_context(identity, &state.config, deadline)?;
            let result =
                owner_result(context.deadline, state.backend.snapshot(context, request)).await?;
            serialize_response(result, permit).await
        }
    }
}

fn validate_headers(headers: &HeaderMap) -> Result<()> {
    let unsupported = || Error::new(ErrorCode::UnsupportedMediaType);
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let value = values.next().ok_or_else(unsupported)?;
    if values.next().is_some() {
        return Err(unsupported());
    }
    let media: mime::Mime = value
        .to_str()
        .map_err(|_| unsupported())?
        .parse()
        .map_err(|_| unsupported())?;
    if media.type_() != mime::APPLICATION
        || media.subtype() != mime::JSON
        || media.suffix().is_some()
    {
        return Err(unsupported());
    }
    for (name, value) in media.params() {
        if name == mime::CHARSET && !value.as_str().eq_ignore_ascii_case("utf-8") {
            return Err(unsupported());
        }
    }
    let mut encodings = headers.get_all(header::CONTENT_ENCODING).iter();
    if let Some(value) = encodings.next()
        && (encodings.next().is_some()
            || !value
                .to_str()
                .map_err(|_| unsupported())?
                .trim()
                .eq_ignore_ascii_case("identity"))
    {
        return Err(unsupported());
    }
    Ok(())
}

async fn collect_body(mut body: Body) -> Result<Vec<u8>> {
    if body.size_hint().lower() > MAX_BODY_BYTES as u64 {
        return Err(Error::new(ErrorCode::PayloadTooLarge));
    }
    let mut bytes = Vec::new();
    while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        let frame = frame.map_err(|_| Error::bad_request())?;
        if let Ok(chunk) = frame.into_data() {
            if chunk.len() > MAX_BODY_BYTES - bytes.len() {
                return Err(Error::new(ErrorCode::PayloadTooLarge));
            }
            bytes.extend_from_slice(&chunk);
        }
    }
    Ok(bytes)
}

async fn parse_body<T: Send + 'static>(
    body: Vec<u8>,
    parse: fn(&[u8]) -> Result<T>,
    permit: Arc<OwnedSemaphorePermit>,
) -> Result<T> {
    tokio::task::spawn_blocking(move || {
        // A timed-out exchange cannot cancel a running blocking task. Keep its
        // admission permit until that bounded parsing work actually finishes.
        let _permit = permit;
        parse(&body)
    })
    .await
    .map_err(|_| Error::internal())?
}

fn owner_context(
    identity: AuthIdentity,
    config: &HttpConfig,
    deadline: Instant,
) -> Result<Context> {
    let now = Instant::now();
    if now >= deadline {
        return Err(Error::internal());
    }
    Ok(Context {
        subject: identity.subject,
        auth_proof: identity.proof,
        deadline: now
            .checked_add(config.transaction_timeout)
            .ok_or_else(Error::internal)?
            .min(deadline),
    })
}

async fn owner_result<T>(deadline: Instant, work: impl Future<Output = Result<T>>) -> Result<T> {
    let result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), work).await;
    if Instant::now() >= deadline {
        return Err(Error::internal());
    }
    result.map_err(|_| Error::internal())?
}

async fn serialize_response<T: Serialize + Send + 'static>(
    value: T,
    permit: Arc<OwnedSemaphorePermit>,
) -> Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut writer = BoundedJson(Vec::new());
        serde_json::to_writer(&mut writer, &value).map_err(|_| Error::internal())?;
        Ok(writer.0)
    })
    .await
    .map_err(|_| Error::internal())?
}

struct BoundedJson(Vec<u8>);

impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_BODY_BYTES - self.0.len() {
            return Err(io::Error::other("response exceeds the protocol body limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn error_response(error: Error) -> Response {
    let status = StatusCode::from_u16(error.status()).expect("protocol error has a valid status");
    error_response_with_status(error, status)
}

fn error_response_with_status(error: Error, status: StatusCode) -> Response {
    let retry_after = error.retry_after.or_else(|| {
        matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
        )
        .then_some(1)
    });
    // Error's serializer contains only protocol enums, UUIDs, and optional
    // fields; it cannot expose a backend's internal exception text.
    let body = serde_json::to_vec(&error).expect("protocol errors serialize to JSON");
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    // Host telemetry tells expected backpressure from failures by the code.
    response.extensions_mut().insert(error.error);
    if let Some(seconds) = retry_after {
        response.headers_mut().insert(
            header::RETRY_AFTER,
            HeaderValue::from_str(&seconds.max(1).to_string()).expect("integer header value"),
        );
    }
    response
}

async fn not_found() -> Response {
    error_response_with_status(Error::bad_request(), StatusCode::NOT_FOUND)
}

async fn method_not_allowed() -> Response {
    error_response_with_status(Error::bad_request(), StatusCode::METHOD_NOT_ALLOWED)
}

async fn protocol_headers(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}
