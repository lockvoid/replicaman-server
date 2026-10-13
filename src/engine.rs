//! Protocol algorithms run inside the host's transaction. No database or ORM.
use crate::{Error, ErrorCode, Result, backend::Context, wire::*};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};
use uuid::Uuid;

pub const LOG_RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;
pub const SNAPSHOT_IDLE_MS: u64 = 600_000;
pub const SNAPSHOT_LIFETIME_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum Accepted {
    Applied,
    Noop { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Accepted {
        cursor: Position,
        #[serde(flatten)]
        result: Accepted,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushResponse {
    pub stream: String,
    pub epoch: Uuid,
    pub op: Uuid,
    #[serde(flatten)]
    pub outcome: Outcome,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EventValue {
    Change { message: CloudEvent },
    Noop { message: (), reason: String },
}
impl EventValue {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Change { message } => message.validate(),
            Self::Noop { reason, .. } => validate_code(reason),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub cursor: Position,
    #[serde(deserialize_with = "crate::wire::deserialize_required_option")]
    pub op: Option<Uuid>,
    #[serde(deserialize_with = "crate::wire::deserialize_required_option")]
    pub hash: Option<String>,
    #[serde(flatten)]
    pub value: EventValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullResponse {
    pub stream: String,
    pub epoch: Uuid,
    pub events: Vec<Event>,
    pub cursor: Position,
    pub to: Position,
    pub head: Position,
    pub more: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Coverage {
    Accepted {
        cursor: Position,
        #[serde(flatten)]
        result: Accepted,
    },
    Rejected {
        reason: String,
    },
    Unseen,
    Mismatch,
}
impl From<Outcome> for Coverage {
    fn from(value: Outcome) -> Self {
        match value {
            Outcome::Accepted { cursor, result } => Self::Accepted { cursor, result },
            Outcome::Rejected { reason } => Self::Rejected { reason },
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotOp {
    pub op: Uuid,
    pub hash: String,
    #[serde(flatten)]
    pub coverage: Coverage,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotResponse {
    pub stream: String,
    pub epoch: Uuid,
    pub client: Uuid,
    pub attempt: Uuid,
    pub snapshot: Uuid,
    pub cursor: Position,
    pub items: Vec<CloudEvent>,
    #[serde(deserialize_with = "crate::wire::deserialize_required_option")]
    pub next: Option<String>,
    pub ttl: u16,
    pub ops: Vec<SnapshotOp>,
}

#[derive(Clone, Copy, Debug)]
pub struct StreamState {
    pub epoch: Uuid,
    pub head: Position,
    pub gc_floor: Position,
}
#[derive(Clone, Copy, Debug)]
pub struct Access {
    pub read: bool,
    pub write: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredReceipt {
    pub op: Uuid,
    pub author: String,
    pub hash: String,
    pub outcome: Outcome,
}
#[derive(Clone, Debug)]
pub struct StoredEvent {
    pub cursor: Position,
    pub author: Option<String>,
    pub op: Option<Uuid>,
    pub hash: Option<String>,
    pub value: EventValue,
}
impl StoredEvent {
    pub fn project(&self, subject: &str) -> Event {
        let own = self.author.as_deref() == Some(subject);
        Event {
            cursor: self.cursor,
            op: if own { self.op } else { None },
            hash: if own { self.hash.clone() } else { None },
            value: self.value.clone(),
        }
    }
}

/// All calls refer to one locked/consistent stream transaction. The backend
/// returns stream_not_found/stream_gone without disclosing unauthorized data.
pub trait ReadTransaction {
    fn state(&mut self) -> Result<StreamState>;
    fn access(&mut self, subject: &str) -> Result<Access>;
    /// Complete first `limit` events in (after, through], ordered by commit.
    fn events(
        &mut self,
        after: Position,
        through: Position,
        limit: usize,
    ) -> Result<Vec<StoredEvent>>;
    fn receipt(&mut self, op: Uuid) -> Result<Option<StoredReceipt>>;
}

#[derive(Clone, Debug)]
pub enum ApplicationOutcome {
    Applied(CloudEvent),
    Noop(String),
    Rejected(String),
}
/// The host commits the whole transaction only on Ok, and rolls back on Err or
/// panic. Savepoints include domain data AND external jobs. Implement admission
/// for all writers; validate_state checks application snapshot/size invariants.
pub trait WriteTransaction: ReadTransaction {
    fn admit(&mut self, subject: &str) -> Result<()>;
    fn begin_application(&mut self) -> Result<()>;
    fn apply(&mut self, message: &CloudEvent) -> Result<ApplicationOutcome>;
    fn rollback_application(&mut self) -> Result<()>;
    fn validate_state(&mut self) -> Result<()>;
    /// Record event+receipt and advance head in this SAME transaction. None
    /// means a rejection/cancellation, with no event and no head advancement.
    fn record(&mut self, event: Option<StoredEvent>, receipt: StoredReceipt) -> Result<()>;
}

/// Validate authoritative metadata and read access before revealing an epoch.
pub fn authorize<T: ReadTransaction>(
    tx: &mut T,
    context: &Context,
    epoch: Uuid,
) -> Result<(StreamState, Access)> {
    context.check()?;
    let access = tx.access(&context.subject)?;
    if !access.read {
        return Err(Error::new(ErrorCode::StreamForbidden));
    }
    let state = tx.state()?;
    if state.epoch != epoch {
        return Err(Error::epoch_changed(state.epoch));
    }
    if state.gc_floor > state.head || state.head.0 > i64::MAX as u64 {
        return Err(Error::internal());
    }
    Ok((state, access))
}

pub fn push<T: WriteTransaction>(
    tx: &mut T,
    context: &Context,
    request: &PushRequest,
) -> Result<PushResponse> {
    validate_push(request)?;
    let (state, access) = authorize(tx, context, request.epoch)?;
    let (op, hash) = match &request.operation {
        PushOperation::Message(message) => (message.id(), message.hash()),
        PushOperation::Cancel { op, hash } => (*op, hash.clone()),
    };
    if let Some(receipt) = tx.receipt(op)? {
        if receipt.op != op {
            return Err(Error::internal());
        }
        if receipt.author != context.subject || receipt.hash != hash {
            return Err(Error::new(ErrorCode::OperationChanged));
        }
        context.check()?;
        return Ok(PushResponse {
            stream: request.stream.clone(),
            epoch: request.epoch,
            op,
            outcome: receipt.outcome,
        });
    }
    tx.admit(&context.subject)?;
    let mut event = None;
    let outcome = match &request.operation {
        PushOperation::Cancel { .. } => Outcome::Rejected {
            reason: "cancelled".into(),
        },
        PushOperation::Message(message) => {
            if !access.write {
                return Err(Error::new(ErrorCode::WriteForbidden));
            }
            tx.begin_application()?;
            let change = tx.apply(message)?;
            match change {
                ApplicationOutcome::Rejected(reason) => {
                    application_reason(&reason)?;
                    tx.rollback_application()?;
                    Outcome::Rejected { reason }
                }
                other => {
                    let next = state
                        .head
                        .0
                        .checked_add(1)
                        .filter(|n| *n <= i64::MAX as u64)
                        .ok_or_else(|| Error::new(ErrorCode::StoragePressure))?;
                    let cursor = Position(next);
                    let (value, result) = match other {
                        ApplicationOutcome::Applied(message) => {
                            message.validate()?;
                            tx.validate_state()?;
                            let message = message.with_id(Uuid::new_v4())?;
                            (EventValue::Change { message }, Accepted::Applied)
                        }
                        ApplicationOutcome::Noop(reason) => {
                            application_reason(&reason)?;
                            tx.rollback_application()?;
                            (
                                EventValue::Noop {
                                    message: (),
                                    reason: reason.clone(),
                                },
                                Accepted::Noop { reason },
                            )
                        }
                        ApplicationOutcome::Rejected(_) => unreachable!(),
                    };
                    event = Some(StoredEvent {
                        cursor,
                        author: Some(context.subject.clone()),
                        op: Some(op),
                        hash: Some(hash.clone()),
                        value,
                    });
                    Outcome::Accepted { cursor, result }
                }
            }
        }
    };
    context.check()?;
    tx.record(
        event,
        StoredReceipt {
            op,
            author: context.subject.clone(),
            hash,
            outcome: outcome.clone(),
        },
    )?;
    context.check()?;
    Ok(PushResponse {
        stream: request.stream.clone(),
        epoch: request.epoch,
        op,
        outcome,
    })
}

fn application_reason(reason: &str) -> Result<()> {
    validate_code(reason).map_err(|_| Error::internal())?;
    if reason == "cancelled" {
        return Err(Error::internal());
    }
    Ok(())
}

pub fn pull<T: ReadTransaction>(
    tx: &mut T,
    context: &Context,
    request: &PullRequest,
) -> Result<PullResponse> {
    validate_pull(request)?;
    let (state, _) = authorize(tx, context, request.epoch)?;
    let to = request.to.unwrap_or(state.head);
    if request.cursor > state.head || to > state.head || to < request.cursor {
        return Err(Error::new(ErrorCode::CursorInvalid));
    }
    if request.cursor < state.gc_floor {
        return Err(Error::new(ErrorCode::CursorExpired));
    }
    let records = tx.events(request.cursor, to, request.limit + 1)?;
    if records.len() > request.limit + 1 {
        return Err(Error::internal());
    }
    let mut previous = request.cursor;
    for record in &records {
        if record.cursor <= previous
            || record.cursor > to
            || record.op.is_some() != record.hash.is_some()
            || (record.op.is_some() && record.author.is_none())
        {
            return Err(Error::internal());
        }
        record.value.validate().map_err(|_| Error::internal())?;
        if let Some(op) = record.op {
            validate_uuid(op).map_err(|_| Error::internal())?;
        }
        if let Some(hash) = &record.hash {
            validate_hash(hash).map_err(|_| Error::internal())?;
        }
        previous = record.cursor;
    }
    let mut response = PullResponse {
        stream: request.stream.clone(),
        epoch: request.epoch,
        events: vec![],
        cursor: to,
        to,
        head: state.head,
        more: false,
    };
    for (i, record) in records.iter().take(request.limit).enumerate() {
        response.events.push(record.project(&context.subject));
        response.more = i + 1 < records.len();
        response.cursor = if response.more { record.cursor } else { to };
        let size = json_size(&response)?;
        if size > PAGE_BYTES {
            if response.events.len() > 1 {
                response.events.pop();
                response.more = true;
                response.cursor = response.events.last().ok_or_else(Error::internal)?.cursor;
            }
            if json_size(&response)? > MAX_BODY_BYTES {
                return Err(Error::internal());
            }
            break;
        }
    }
    context.check()?;
    Ok(response)
}

pub fn coverage<T: ReadTransaction>(
    tx: &mut T,
    subject: &str,
    pending: &[Pending],
) -> Result<Vec<SnapshotOp>> {
    pending
        .iter()
        .map(|p| {
            let coverage = match tx.receipt(p.op)? {
                None => Coverage::Unseen,
                Some(r) if r.op != p.op => return Err(Error::internal()),
                Some(r) if r.author != subject || r.hash != p.hash => Coverage::Mismatch,
                Some(r) => r.outcome.into(),
            };
            Ok(SnapshotOp {
                op: p.op,
                hash: p.hash.clone(),
                coverage,
            })
        })
        .collect()
}

/// Counts encoded bytes without allocating another body-sized buffer.
pub fn json_size(value: &impl Serialize) -> Result<usize> {
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).map_err(|_| Error::internal())?;
    Ok(counter.0)
}
