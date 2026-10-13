//! Replicaman's wire codec, transaction algorithms and embeddable HTTP server.
//!
//! The host owns authentication, application data and durable transactions.
//! See `docs/SERVER.md` for the backend contract and the runnable example.
#![forbid(unsafe_code)]

pub mod backend;
pub mod engine;
pub mod error;
pub mod http;
pub mod snapshot;
pub mod wire;

pub use error::{Error, ErrorCode, Result};
