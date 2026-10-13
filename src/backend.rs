//! The host's atomic boundary. An implementation may use a local database or
//! send each complete command to its data owner over RPC. Never split a push's
//! application write, log append and receipt across independent commits.
use crate::engine::{PullResponse, PushResponse, SnapshotResponse};
use crate::wire::{PullRequest, PushRequest, SnapshotRequest};
use crate::{Error, ErrorCode, Result};
use std::{fmt, future::Future, sync::Arc, time::Instant};

/// Request-scoped credentials for the trusted owner. Never logged by Debug.
#[derive(Clone)]
pub struct AuthProof(Arc<[u8]>);
impl AuthProof {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into().into())
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
impl fmt::Debug for AuthProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthProof([REDACTED])")
    }
}

#[derive(Clone, Debug)]
pub struct AuthIdentity {
    pub subject: String,
    pub proof: Option<AuthProof>,
}
impl From<String> for AuthIdentity {
    fn from(subject: String) -> Self {
        Self {
            subject,
            proof: None,
        }
    }
}
impl From<&str> for AuthIdentity {
    fn from(subject: &str) -> Self {
        subject.to_owned().into()
    }
}

#[derive(Clone, Debug)]
pub struct Context {
    /// Stable authenticated subject, not the bearer token or CloudEvents source.
    pub subject: String,
    /// The proof returned by authentication for this exact exchange.
    pub auth_proof: Option<AuthProof>,
    /// The data owner must fence expired work before committing it.
    pub deadline: Instant,
}
impl Context {
    pub fn check(&self) -> Result<()> {
        if self.subject.is_empty() {
            return Err(Error::new(ErrorCode::Unauthorized));
        }
        if Instant::now() >= self.deadline {
            return Err(Error::internal());
        }
        Ok(())
    }
}

/// Trusted host callbacks. Each call checks current read rights and epoch in
/// the SAME authoritative transaction as its read/write. Dropped futures are
/// not proof of rollback: retry with the original operation ID after a timeout.
///
/// `engine` supplies the protocol decisions to run inside those transactions.
/// Backends must roll back on Err/panic and enforce Context::deadline at the
/// data owner. Snapshot cuts and publication must satisfy docs/IMPLEMENTATION.md.
pub trait Backend: Send + Sync + 'static {
    fn push(
        &self,
        context: Context,
        request: PushRequest,
    ) -> impl Future<Output = Result<PushResponse>> + Send;
    fn pull(
        &self,
        context: Context,
        request: PullRequest,
    ) -> impl Future<Output = Result<PullResponse>> + Send;
    fn snapshot(
        &self,
        context: Context,
        request: SnapshotRequest,
    ) -> impl Future<Output = Result<SnapshotResponse>> + Send;
}
