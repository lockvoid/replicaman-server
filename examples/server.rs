//! Run with `REPLICAMAN_DEMO_TOKEN=<local-secret> cargo run --example server`.
//! Loopback HTTP for development ONLY. Production hosts provide durable storage,
//! authentication, authorization, HTTPS, admission and the documented deadlines.
#[path = "support/shutdown.rs"]
mod shutdown;
mod support;

use axum::http::{HeaderMap, header};
use replicaman_server::{
    Error, ErrorCode, Result,
    http::{AuthIdentity, Authenticator, router},
};
use support::MemoryBackend;

struct Bearer {
    expected: String,
}
impl Authenticator for Bearer {
    async fn authenticate(&self, headers: &HeaderMap) -> Result<AuthIdentity> {
        let supplied = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        if supplied != Some(self.expected.as_str()) {
            return Err(Error::new(ErrorCode::Unauthorized));
        }
        // A stable subject; rotating the local token does not change its identity.
        Ok("demo-user".into())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("REPLICAMAN_DEMO_TOKEN")
        .map_err(|_| "set REPLICAMAN_DEMO_TOKEN to a local development secret")?;
    if token.is_empty() || token.chars().any(char::is_whitespace) {
        return Err("REPLICAMAN_DEMO_TOKEN must be nonempty and contain no whitespace".into());
    }
    let stream = "demo/kv";
    let backend = MemoryBackend::new(stream);
    backend.grant("demo-user", "demo-account", true, true).await;
    let epoch = backend.epoch().await;
    let bind = std::env::var("REPLICAMAN_DEMO_BIND").unwrap_or_else(|_| "127.0.0.1:4100".into());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let address = listener.local_addr()?;
    eprintln!(
        "DEVELOPMENT ONLY: all in-memory data is lost on restart; every start creates a new epoch."
    );
    eprintln!(
        "Bootstrap: {}",
        serde_json::json!({"stream": stream, "epoch": epoch})
    );
    eprintln!("Listening at http://{address}; use Authorization: Bearer <REPLICAMAN_DEMO_TOKEN>.");
    eprintln!(
        "Payload schema and canonicalization: examples/support/mod.rs. No external jobs are dispatched."
    );
    axum::serve(
        listener,
        router(
            backend,
            Bearer {
                expected: format!("Bearer {token}"),
            },
        ),
    )
    .with_graceful_shutdown(shutdown::signal())
    .await?;
    Ok(())
}
