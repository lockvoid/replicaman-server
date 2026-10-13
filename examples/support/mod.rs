//! DEMO/TEST ONLY: a process-local data owner, not a production database.
//!
//! Clone a candidate under one mutex, run the engine, and publish only on Ok
//! before the owner's deadline. Errors and panics discard the entire candidate.
//! Application savepoints include values AND jobs. Snapshot copying under this
//! mutex is deliberately simple; production owners need the consistent-cut and
//! bounded staging architecture described in docs/IMPLEMENTATION.md.
//!
//! Application type `replicaman.demo.kv.v1`, JSON payloads:
//! `{"action":"set","key":"tea","value":" oolong "}` or
//! `{"action":"delete","key":"tea"}`. Values are trimmed before acceptance;
//! canonical events carry the normalized transition. Repeating the current
//! value, or deleting a missing key, is an explicit noop. Snapshot items are set
//! transitions, applied to an empty map. Jobs below are recorded, never sent.
#![allow(dead_code)] // Shared by the runnable example and integration tests.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use replicaman_server::{
    Error, ErrorCode, Result,
    backend::{Backend, Context},
    engine::{
        self, Access, ApplicationOutcome, EventValue, PullResponse, PushResponse, ReadTransaction,
        SnapshotResponse, StoredEvent, StoredReceipt, StreamState, WriteTransaction,
    },
    snapshot::{Snapshot, SnapshotCut, SnapshotLimits, TokenKey},
    wire::{CloudEvent, Position, PullRequest, PushRequest, SnapshotKind, SnapshotRequest},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use uuid::Uuid;

pub const APPLICATION_TYPE: &str = "replicaman.demo.kv.v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Change {
    Set { key: String, value: String },
    Delete { key: String },
}

pub fn message(id: Uuid, change: &Change) -> CloudEvent {
    CloudEvent::from_value(json!({
        "specversion": "1.0", "id": id, "source": "urn:replicaman:demo",
        "type": APPLICATION_TYPE, "datacontenttype": "application/json",
        "data_base64": STANDARD.encode(serde_json::to_vec(change).expect("demo JSON"))
    }))
    .expect("valid demo CloudEvent")
}

pub fn decode(message: &CloudEvent) -> Result<Change> {
    let attributes = message.as_value();
    if attributes["type"] != APPLICATION_TYPE || attributes["datacontenttype"] != "application/json"
    {
        return Err(Error::new(ErrorCode::UnsupportedFormat));
    }
    serde_json::from_slice(&message.data()?).map_err(|_| Error::new(ErrorCode::UnsupportedFormat))
}

/// The same canonical transition is used to reconstruct snapshots plus tails.
pub fn apply_canonical(values: &mut BTreeMap<String, String>, message: &CloudEvent) -> Result<()> {
    match decode(message)? {
        Change::Set { key, value } => {
            values.insert(key, value);
        }
        Change::Delete { key } => {
            values.remove(&key);
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub enum Fault {
    #[default]
    None,
    RejectAfterWrite,
    ErrorAfterWrite,
    PanicAfterWrite,
    NoopAfterWrite,
    DelayAfterWrite(Duration),
}

#[derive(Clone)]
struct Grant {
    account: String,
    access: Access,
}
#[derive(Clone)]
struct LogEntry {
    event: StoredEvent,
    // First observed AFTER commit by GC: conservative 30-day retention.
    retained_since: Option<u64>,
}
#[derive(Clone)]
struct CachedSnapshot {
    account: String,
    image: Snapshot,
}

#[derive(Clone)]
pub struct Limits {
    pub global_snapshots: usize,
    pub account_snapshots: usize,
    pub global_receipts: usize,
    pub account_receipts: usize,
    pub log_events: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            global_snapshots: 8,
            account_snapshots: 4,
            global_receipts: 4096,
            account_receipts: 1024,
            log_events: 4096,
        }
    }
}

#[derive(Clone)]
struct State {
    stream: String,
    epoch: Uuid,
    head: Position,
    gc_floor: Position,
    values: BTreeMap<String, String>,
    jobs: Vec<Change>,
    apply_calls: usize,
    log: Vec<LogEntry>,
    receipts: HashMap<Uuid, StoredReceipt>,
    receipt_accounts: HashMap<Uuid, String>,
    grants: HashMap<String, Grant>,
    snapshots: Vec<CachedSnapshot>,
    token_key: TokenKey,
    limits: Limits,
    fault: Fault,
}

#[derive(Clone)]
enum Clock {
    Live(Instant),
    Manual(Arc<AtomicU64>),
}
impl Clock {
    fn now_ms(&self) -> u64 {
        match self {
            Self::Live(origin) => u64::try_from(origin.elapsed().as_millis()).unwrap_or(u64::MAX),
            Self::Manual(now) => now.load(Ordering::SeqCst),
        }
    }
}

#[derive(Clone)]
pub struct MemoryBackend {
    inner: Arc<Mutex<State>>,
    clock: Clock,
    snapshot_build_budget: Duration,
}

#[derive(Clone, Debug)]
pub struct View {
    pub epoch: Uuid,
    pub head: Position,
    pub gc_floor: Position,
    pub values: BTreeMap<String, String>,
    pub jobs: Vec<Change>,
    pub apply_calls: usize,
    pub receipts: usize,
    pub events: Vec<StoredEvent>,
    pub snapshots: usize,
}

impl MemoryBackend {
    pub fn new(stream: impl Into<String>) -> Self {
        Self::with_clock(stream.into(), Clock::Live(Instant::now()))
    }
    pub fn manual(stream: impl Into<String>) -> Self {
        Self::with_clock(stream.into(), Clock::Manual(Arc::new(AtomicU64::new(0))))
    }
    fn with_clock(stream: String, clock: Clock) -> Self {
        let state = State {
            stream,
            epoch: Uuid::new_v4(),
            head: Position(0),
            gc_floor: Position(0),
            values: BTreeMap::new(),
            jobs: vec![],
            apply_calls: 0,
            log: vec![],
            receipts: HashMap::new(),
            receipt_accounts: HashMap::new(),
            grants: HashMap::new(),
            snapshots: vec![],
            token_key: TokenKey::random(),
            limits: Limits::default(),
            fault: Fault::None,
        };
        Self {
            inner: Arc::new(Mutex::new(state)),
            clock,
            snapshot_build_budget: Duration::from_secs(10),
        }
    }
    /// Allow deterministic budget exhaustion in tests without a 10-second wait.
    pub fn with_snapshot_build_budget(mut self, budget: Duration) -> Self {
        self.snapshot_build_budget = budget;
        self
    }
    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }
    pub fn advance_ms(&self, milliseconds: u64) {
        match &self.clock {
            Clock::Manual(now) => {
                now.fetch_add(milliseconds, Ordering::SeqCst);
            }
            Clock::Live(_) => panic!("only a test clock can be advanced"),
        }
    }
    pub async fn grant(&self, subject: &str, account: &str, read: bool, write: bool) {
        self.inner.lock().await.grants.insert(
            subject.into(),
            Grant {
                account: account.into(),
                access: Access { read, write },
            },
        );
    }
    pub async fn limits(&self, limits: Limits) {
        self.inner.lock().await.limits = limits;
    }
    pub async fn fault(&self, fault: Fault) {
        self.inner.lock().await.fault = fault;
    }
    pub async fn epoch(&self) -> Uuid {
        self.inner.lock().await.epoch
    }
    pub async fn view(&self) -> View {
        let state = self.inner.lock().await;
        View {
            epoch: state.epoch,
            head: state.head,
            gc_floor: state.gc_floor,
            values: state.values.clone(),
            jobs: state.jobs.clone(),
            apply_calls: state.apply_calls,
            receipts: state.receipts.len(),
            events: state.log.iter().map(|e| e.event.clone()).collect(),
            snapshots: state.snapshots.len(),
        }
    }

    async fn transaction<T: Send>(
        &self,
        context: Context,
        stream: String,
        body: impl FnOnce(&mut Transaction<'_>, &Context) -> Result<T> + Send,
    ) -> Result<T> {
        context.check()?;
        let mut committed = tokio::time::timeout_at(context.deadline.into(), self.inner.lock())
            .await
            .map_err(|_| Error::internal())?;
        context.check()?;
        let mut candidate = committed.clone();
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut tx = Transaction {
                state: &mut candidate,
                stream: &stream,
                deadline: context.deadline,
                savepoint: None,
            };
            body(&mut tx, &context)
        }))
        .map_err(|_| Error::internal())??;
        // The data owner fences late completion even if HTTP already timed out.
        context.check()?;
        *committed = candidate;
        Ok(result)
    }

    /// Test/admin maintenance: mark newly visible commits first; only a later
    /// pass at least 30 days after that mark may remove their contiguous prefix.
    pub async fn gc(&self) {
        let mut state = self.inner.lock().await;
        let now = self.now_ms();
        for entry in &mut state.log {
            entry.retained_since.get_or_insert(now);
        }
        let count = state
            .log
            .iter()
            .take_while(|entry| {
                now.saturating_sub(entry.retained_since.expect("marked"))
                    >= engine::LOG_RETENTION_MS
            })
            .count();
        if count > 0 {
            state.gc_floor = state.log[count - 1].event.cursor;
            state.log.drain(..count);
        }
    }

    /// Simulate loss/replacement of storage. All clones share the fencing epoch.
    /// Real deployments persist this boundary and fence the previous owner.
    pub async fn new_epoch(&self) -> Uuid {
        let mut state = self.inner.lock().await;
        state.epoch = Uuid::new_v4();
        state.head = Position(0);
        state.gc_floor = Position(0);
        state.values.clear();
        state.jobs.clear();
        state.log.clear();
        state.receipts.clear();
        state.receipt_accounts.clear();
        state.snapshots.clear();
        state.apply_calls = 0;
        state.token_key = TokenKey::random();
        state.epoch
    }

    pub async fn lose_snapshot_staging(&self) {
        self.inner.lock().await.snapshots.clear();
    }

    /// Another writer uses the SAME admission, application and commit boundary,
    /// publishing an event without client op/hash metadata or a client receipt.
    pub async fn server_write(
        &self,
        context: Context,
        stream: String,
        epoch: Uuid,
        message: CloudEvent,
    ) -> Result<Position> {
        self.transaction(context, stream, move |tx, context| {
            let (state, access) = engine::authorize(tx, context, epoch)?;
            if !access.write {
                return Err(Error::new(ErrorCode::WriteForbidden));
            }
            tx.admit(&context.subject)?;
            tx.begin_application()?;
            let value = match tx.apply(&message)? {
                ApplicationOutcome::Applied(message) => {
                    tx.validate_state()?;
                    EventValue::Change {
                        message: message.with_id(Uuid::new_v4())?,
                    }
                }
                ApplicationOutcome::Noop(reason) => {
                    tx.rollback_application()?;
                    EventValue::Noop {
                        message: (),
                        reason,
                    }
                }
                ApplicationOutcome::Rejected(_) => return Err(Error::bad_request()),
            };
            let cursor = Position(
                state
                    .head
                    .0
                    .checked_add(1)
                    .filter(|p| *p <= i64::MAX as u64)
                    .ok_or_else(|| Error::new(ErrorCode::StoragePressure))?,
            );
            tx.state.log.push(LogEntry {
                event: StoredEvent {
                    cursor,
                    author: None,
                    op: None,
                    hash: None,
                    value,
                },
                retained_since: None,
            });
            tx.state.head = cursor;
            Ok(cursor)
        })
        .await
    }
}

struct Transaction<'a> {
    state: &'a mut State,
    stream: &'a str,
    deadline: Instant,
    savepoint: Option<(BTreeMap<String, String>, Vec<Change>)>,
}
impl ReadTransaction for Transaction<'_> {
    fn state(&mut self) -> Result<StreamState> {
        if self.stream != self.state.stream {
            return Err(Error::new(ErrorCode::StreamNotFound));
        }
        Ok(StreamState {
            epoch: self.state.epoch,
            head: self.state.head,
            gc_floor: self.state.gc_floor,
        })
    }
    fn access(&mut self, subject: &str) -> Result<Access> {
        Ok(self
            .state
            .grants
            .get(subject)
            .map(|g| g.access)
            .unwrap_or(Access {
                read: false,
                write: false,
            }))
    }
    fn events(
        &mut self,
        after: Position,
        through: Position,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        Ok(self
            .state
            .log
            .iter()
            .filter(|entry| entry.event.cursor > after && entry.event.cursor <= through)
            .take(limit)
            .map(|entry| entry.event.clone())
            .collect())
    }
    fn receipt(&mut self, op: Uuid) -> Result<Option<StoredReceipt>> {
        Ok(self.state.receipts.get(&op).cloned())
    }
}
impl WriteTransaction for Transaction<'_> {
    fn admit(&mut self, subject: &str) -> Result<()> {
        let account = &self
            .state
            .grants
            .get(subject)
            .ok_or_else(|| Error::new(ErrorCode::StreamForbidden))?
            .account;
        let account_count = self
            .state
            .receipt_accounts
            .values()
            .filter(|a| *a == account)
            .count();
        if self.state.receipts.len() >= self.state.limits.global_receipts
            || account_count >= self.state.limits.account_receipts
            || self.state.log.len() >= self.state.limits.log_events
        {
            return Err(Error::new(ErrorCode::StoragePressure));
        }
        Ok(())
    }
    fn begin_application(&mut self) -> Result<()> {
        self.savepoint = Some((self.state.values.clone(), self.state.jobs.clone()));
        Ok(())
    }
    fn apply(&mut self, input: &CloudEvent) -> Result<ApplicationOutcome> {
        let change = decode(input)?;
        let key = match &change {
            Change::Set { key, .. } | Change::Delete { key } => key,
        };
        if key.is_empty()
            || key.len() > 128
            || !key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._/-".contains(&b))
        {
            return Ok(ApplicationOutcome::Rejected("invalid_key".into()));
        }
        self.state.apply_calls += 1;
        let canonical = match change {
            Change::Set { key, value } => {
                let value = value.trim().to_owned();
                if self.state.values.get(&key) == Some(&value) {
                    return Ok(ApplicationOutcome::Noop("unchanged".into()));
                }
                Change::Set { key, value }
            }
            Change::Delete { key } => {
                if !self.state.values.contains_key(&key) {
                    return Ok(ApplicationOutcome::Noop("missing_key".into()));
                }
                Change::Delete { key }
            }
        };
        let result = message(Uuid::new_v4(), &canonical);
        apply_canonical(&mut self.state.values, &result)?;
        self.state.jobs.push(canonical);
        // A deterministic application-size violation is a durable rejection;
        // the engine restores this savepoint, including the candidate job.
        if let Err(error) = self.validate_state() {
            if error.error == ErrorCode::StoragePressure {
                return Ok(ApplicationOutcome::Rejected("size_limit".into()));
            }
            return Err(error);
        }
        match &self.state.fault {
            Fault::None => (),
            Fault::RejectAfterWrite => {
                return Ok(ApplicationOutcome::Rejected("demo_rejected".into()));
            }
            Fault::ErrorAfterWrite => return Err(Error::internal()),
            Fault::PanicAfterWrite => {
                panic!("injected application panic; candidate must roll back")
            }
            Fault::NoopAfterWrite => return Ok(ApplicationOutcome::Noop("demo_noop".into())),
            Fault::DelayAfterWrite(delay) => {
                // A cooperative test delay, not preemption of arbitrary
                // synchronous application code. Never hold the owner lock for
                // the full injected delay after its transaction has expired.
                let remaining = self.deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(Error::internal());
                }
                std::thread::sleep((*delay).min(remaining));
                if Instant::now() >= self.deadline {
                    return Err(Error::internal());
                }
            }
        }
        Ok(ApplicationOutcome::Applied(result))
    }
    fn rollback_application(&mut self) -> Result<()> {
        let (values, jobs) = self.savepoint.take().ok_or_else(Error::internal)?;
        self.state.values = values;
        self.state.jobs = jobs;
        Ok(())
    }
    fn validate_state(&mut self) -> Result<()> {
        let limits = SnapshotLimits::default();
        if self.state.values.len() > limits.max_items {
            return Err(Error::new(ErrorCode::StoragePressure));
        }
        let mut total = 0usize;
        for (key, value) in &self.state.values {
            let bytes = serde_json::to_vec(&Change::Set {
                key: key.clone(),
                value: value.clone(),
            })
            .map_err(|_| Error::internal())?
            .len();
            total = total.checked_add(bytes).ok_or_else(Error::internal)?;
            if bytes > limits.max_item_bytes || total > limits.max_data_bytes {
                return Err(Error::new(ErrorCode::StoragePressure));
            }
        }
        Ok(())
    }
    fn record(&mut self, event: Option<StoredEvent>, receipt: StoredReceipt) -> Result<()> {
        if self.state.receipts.contains_key(&receipt.op) {
            return Err(Error::internal());
        }
        if let Some(event) = event {
            if event.cursor.0 != self.state.head.0 + 1 {
                return Err(Error::internal());
            }
            self.state.head = event.cursor;
            self.state.log.push(LogEntry {
                event,
                retained_since: None,
            });
        }
        let account = self
            .state
            .grants
            .get(&receipt.author)
            .ok_or_else(Error::internal)?
            .account
            .clone();
        self.state.receipt_accounts.insert(receipt.op, account);
        self.state.receipts.insert(receipt.op, receipt);
        Ok(())
    }
}

impl Backend for MemoryBackend {
    async fn push(&self, context: Context, request: PushRequest) -> Result<PushResponse> {
        self.transaction(context, request.stream.clone(), move |tx, context| {
            engine::push(tx, context, &request)
        })
        .await
    }
    async fn pull(&self, context: Context, request: PullRequest) -> Result<PullResponse> {
        self.transaction(context, request.stream.clone(), move |tx, context| {
            engine::pull(tx, context, &request)
        })
        .await
    }
    async fn snapshot(
        &self,
        mut context: Context,
        request: SnapshotRequest,
    ) -> Result<SnapshotResponse> {
        let owner_deadline = context.deadline;
        let build_deadline = Instant::now()
            .checked_add(self.snapshot_build_budget)
            .ok_or_else(Error::internal)?;
        context.deadline = owner_deadline.min(build_deadline);
        let clock = self.clock.clone();
        self.transaction(context, request.stream.clone(), move |tx, context| {
            replicaman_server::wire::validate_snapshot(&request)?;
            let (state, _) = engine::authorize(tx, context, request.epoch)?;
            tx.state.token_key.verify(context, &request)?;
            let now = clock.now_ms();
            tx.state.snapshots.retain(|entry| !entry.image.expired(now));
            // Current authority is checked above, even when the cached attempt exists.
            if let Some(entry) = tx
                .state
                .snapshots
                .iter_mut()
                .find(|entry| entry.image.same_attempt(context, &request))
            {
                return entry.image.page(context, &request, now);
            }
            let SnapshotKind::Create { pending, .. } = &request.kind else {
                return Err(Error::new(ErrorCode::SnapshotExpired));
            };
            let account = tx
                .state
                .grants
                .get(&context.subject)
                .ok_or_else(Error::internal)?
                .account
                .clone();
            let same_slot = tx
                .state
                .snapshots
                .iter()
                .find(|entry| entry.image.same_slot(context, &request));
            let same_account: Vec<_> = tx
                .state
                .snapshots
                .iter()
                .filter(|entry| entry.account == account)
                .collect();
            let blocked = same_slot.is_some()
                || tx.state.snapshots.len() >= tx.state.limits.global_snapshots
                || same_account.len() >= tx.state.limits.account_snapshots;
            if blocked {
                let retry_after = if let Some(entry) = same_slot {
                    entry.image.retry_after(now)
                } else {
                    tx.state
                        .snapshots
                        .iter()
                        .map(|entry| entry.image.retry_after(now))
                        .min()
                        .unwrap_or(1)
                };
                return Err(Error::retry(ErrorCode::RateLimited, retry_after));
            }
            let items = tx
                .state
                .values
                .iter()
                .map(|(key, value)| {
                    message(
                        Uuid::new_v4(),
                        &Change::Set {
                            key: key.clone(),
                            value: value.clone(),
                        },
                    )
                })
                .collect();
            let cut = SnapshotCut {
                epoch: state.epoch,
                cursor: state.head,
                at_ms: now,
                items,
            };
            // This demo serializes cut + later receipt publication under one lock.
            let ops = engine::coverage(tx, &context.subject, pending)?;
            let mut image = Snapshot::new(
                context,
                &request,
                cut,
                ops,
                clock.now_ms(),
                SnapshotLimits::default(),
                &tx.state.token_key,
            )?;
            let response = image.page(context, &request, clock.now_ms())?;
            tx.state.snapshots.push(CachedSnapshot { account, image });
            Ok(response)
        })
        .await
        .map_err(|error| {
            // Preserve the earlier owner deadline and unrelated errors. The
            // build budget is temporary snapshot contention, not a receipt or
            // a generic handler failure. transaction() has discarded its image.
            if error.error == ErrorCode::InternalError
                && build_deadline < owner_deadline
                && Instant::now() >= build_deadline
            {
                Error::new(ErrorCode::SnapshotBusy)
            } else {
                error
            }
        })
    }
}
