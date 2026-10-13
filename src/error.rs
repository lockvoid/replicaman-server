use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadRequest,
    UnsupportedProtocol,
    Unauthorized,
    StreamForbidden,
    WriteForbidden,
    StreamNotFound,
    StreamGone,
    EpochChanged,
    CursorExpired,
    SnapshotExpired,
    CursorInvalid,
    AttemptChanged,
    OperationChanged,
    PayloadTooLarge,
    UnsupportedMediaType,
    UnsupportedFormat,
    RateLimited,
    NotCaughtUp,
    StoragePressure,
    SnapshotBusy,
    InternalError,
}

impl ErrorCode {
    pub fn status(self) -> u16 {
        match self {
            Self::BadRequest | Self::UnsupportedProtocol => 400,
            Self::Unauthorized => 401,
            Self::StreamForbidden | Self::WriteForbidden => 403,
            Self::StreamNotFound => 404,
            Self::StreamGone => 410,
            Self::EpochChanged
            | Self::CursorExpired
            | Self::SnapshotExpired
            | Self::CursorInvalid
            | Self::AttemptChanged
            | Self::OperationChanged => 409,
            Self::PayloadTooLarge => 413,
            Self::UnsupportedMediaType => 415,
            Self::UnsupportedFormat => 422,
            Self::RateLimited => 429,
            Self::NotCaughtUp | Self::StoragePressure | Self::SnapshotBusy => 503,
            Self::InternalError => 500,
        }
    }
}

/// Public errors contain only protocol codes, never internal exception details.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    pub error: ErrorCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<Uuid>,
    #[serde(skip)]
    pub retry_after: Option<u64>,
}

impl Error {
    pub fn new(error: ErrorCode) -> Self {
        let retry_after = matches!(
            error,
            ErrorCode::RateLimited
                | ErrorCode::NotCaughtUp
                | ErrorCode::StoragePressure
                | ErrorCode::SnapshotBusy
        )
        .then_some(1);
        Self {
            error,
            epoch: None,
            retry_after,
        }
    }
    pub fn bad_request() -> Self {
        Self::new(ErrorCode::BadRequest)
    }
    pub fn internal() -> Self {
        Self::new(ErrorCode::InternalError)
    }
    pub fn epoch_changed(epoch: Uuid) -> Self {
        Self {
            error: ErrorCode::EpochChanged,
            epoch: Some(epoch),
            retry_after: None,
        }
    }
    pub fn retry(error: ErrorCode, seconds: u64) -> Self {
        Self {
            error,
            epoch: None,
            retry_after: Some(seconds.max(1)),
        }
    }
    pub fn status(&self) -> u16 {
        self.error.status()
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.error)
    }
}
impl std::error::Error for Error {}
