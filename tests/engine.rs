//! Conformance checks at a real atomic owner boundary, using the demo backend.
//! These do not establish correctness of a production database or application.
#[path = "../examples/support/mod.rs"]
mod support;

use replicaman_server::{
    ErrorCode,
    backend::{Backend, Context},
    engine::{Accepted, Coverage, EventValue, LOG_RETENTION_MS, Outcome, SNAPSHOT_IDLE_MS},
    wire::{
        Pending, Position, PullRequest, PushOperation, PushRequest, SnapshotKind, SnapshotRequest,
    },
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use support::{Change, Fault, Limits, MemoryBackend, apply_canonical, decode, message};
use tokio::sync::Barrier;
use uuid::Uuid;

const STREAM: &str = "tests/kv";
fn context(subject: &str) -> Context {
    Context {
        auth_proof: None,
        subject: subject.into(),
        deadline: Instant::now() + Duration::from_secs(30),
    }
}
async fn store() -> MemoryBackend {
    let db = MemoryBackend::manual(STREAM);
    db.grant("alice", "account-a", true, true).await;
    db.grant("bob", "account-b", true, true).await;
    db
}
fn set(epoch: Uuid, key: &str, value: &str) -> PushRequest {
    PushRequest {
        stream: STREAM.into(),
        epoch,
        operation: PushOperation::Message(message(
            Uuid::new_v4(),
            &Change::Set {
                key: key.into(),
                value: value.into(),
            },
        )),
    }
}
fn pending(request: &PushRequest) -> Pending {
    match &request.operation {
        PushOperation::Message(message) => Pending {
            op: message.id(),
            hash: message.hash(),
        },
        PushOperation::Cancel { op, hash } => Pending {
            op: *op,
            hash: hash.clone(),
        },
    }
}
fn cancel(request: &PushRequest) -> PushRequest {
    let Pending { op, hash } = pending(request);
    PushRequest {
        stream: request.stream.clone(),
        epoch: request.epoch,
        operation: PushOperation::Cancel { op, hash },
    }
}
fn pull(epoch: Uuid, cursor: u64, to: Option<u64>, limit: usize) -> PullRequest {
    PullRequest {
        stream: STREAM.into(),
        epoch,
        cursor: Position(cursor),
        to: to.map(Position),
        limit,
    }
}
fn snapshot(epoch: Uuid, pending: Vec<Pending>, limit: usize) -> SnapshotRequest {
    SnapshotRequest {
        stream: STREAM.into(),
        epoch,
        client: Uuid::new_v4(),
        attempt: Uuid::new_v4(),
        kind: SnapshotKind::Create { limit, pending },
    }
}
fn page(request: &SnapshotRequest, snapshot: Uuid, token: String) -> SnapshotRequest {
    SnapshotRequest {
        kind: SnapshotKind::Page {
            snapshot,
            page: token,
            limit: None,
        },
        ..request.clone()
    }
}
async fn push_set(db: &MemoryBackend, epoch: Uuid, key: &str, value: &str) {
    db.push(context("alice"), set(epoch, key, value))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_duplicate_push_executes_once() {
    let db = store().await;
    let request = set(db.epoch().await, "tea", " oolong ");
    let start = Arc::new(Barrier::new(17));
    let mut tasks = vec![];
    for _ in 0..16 {
        let (db, request, start) = (db.clone(), request.clone(), start.clone());
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            db.push(context("alice"), request).await.unwrap()
        }));
    }
    start.wait().await;
    let mut responses = vec![];
    for task in tasks {
        responses.push(task.await.unwrap());
    }
    assert!(responses.iter().all(|r| r == &responses[0]));
    let view = db.view().await;
    assert_eq!(
        (
            view.apply_calls,
            view.receipts,
            view.events.len(),
            view.jobs.len()
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(view.values["tea"], "oolong");
    assert_eq!(view.head, Position(1));
}

#[tokio::test]
async fn changed_content_or_author_cannot_reuse_an_operation() {
    let db = store().await;
    let original = set(db.epoch().await, "tea", "oolong");
    let id = pending(&original).op;
    db.push(context("alice"), original.clone()).await.unwrap();
    let changed = PushRequest {
        operation: PushOperation::Message(message(
            id,
            &Change::Set {
                key: "tea".into(),
                value: "black".into(),
            },
        )),
        ..original.clone()
    };
    assert_eq!(
        db.push(context("alice"), changed).await.unwrap_err().error,
        ErrorCode::OperationChanged
    );
    assert_eq!(
        db.push(context("bob"), original).await.unwrap_err().error,
        ErrorCode::OperationChanged
    );
    assert_eq!(db.view().await.apply_calls, 1);
}

#[tokio::test]
async fn receipt_lookup_precedes_write_rights_handler_and_new_receipt_quota() {
    let db = store().await;
    let request = set(db.epoch().await, "tea", "oolong");
    let accepted = db.push(context("alice"), request.clone()).await.unwrap();
    db.grant("alice", "account-a", true, false).await;
    db.limits(Limits {
        global_receipts: 0,
        account_receipts: 0,
        log_events: 0,
        ..Limits::default()
    })
    .await;
    db.fault(Fault::ErrorAfterWrite).await;
    assert_eq!(
        db.push(context("alice"), request.clone()).await.unwrap(),
        accepted
    );
    assert_eq!(
        db.push(context("alice"), cancel(&request)).await.unwrap(),
        accepted
    );
    assert_eq!(
        db.push(context("alice"), set(request.epoch, "new", "value"))
            .await
            .unwrap_err()
            .error,
        ErrorCode::StoragePressure
    );
    db.grant("alice", "account-a", false, false).await;
    assert_eq!(
        db.push(context("alice"), request).await.unwrap_err().error,
        ErrorCode::StreamForbidden
    );
    assert_eq!(db.view().await.apply_calls, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_cancel_race_has_one_immutable_outcome() {
    let db = store().await;
    let request = set(db.epoch().await, "tea", "oolong");
    let start = Arc::new(Barrier::new(3));
    let push_task = {
        let (db, request, start) = (db.clone(), request.clone(), start.clone());
        tokio::spawn(async move {
            start.wait().await;
            db.push(context("alice"), request).await.unwrap()
        })
    };
    let cancel_task = {
        let (db, request, start) = (db.clone(), cancel(&request), start.clone());
        tokio::spawn(async move {
            start.wait().await;
            db.push(context("alice"), request).await.unwrap()
        })
    };
    start.wait().await;
    let (pushed, cancelled) = (push_task.await.unwrap(), cancel_task.await.unwrap());
    assert_eq!(pushed, cancelled);
    assert_eq!(db.push(context("alice"), request).await.unwrap(), pushed);
    let view = db.view().await;
    assert_eq!(view.receipts, 1);
    match pushed.outcome {
        Outcome::Accepted { .. } => {
            assert_eq!(
                (view.apply_calls, view.events.len(), view.jobs.len()),
                (1, 1, 1)
            );
        }
        Outcome::Rejected { reason } => {
            assert_eq!(reason, "cancelled");
            assert_eq!(
                (view.apply_calls, view.events.len(), view.jobs.len()),
                (0, 0, 0)
            );
        }
    }
}

#[tokio::test]
async fn read_only_cancel_fences_a_delayed_push() {
    let db = store().await;
    let request = set(db.epoch().await, "tea", "oolong");
    db.grant("alice", "account-a", true, false).await;
    let cancelled = db.push(context("alice"), cancel(&request)).await.unwrap();
    assert_eq!(
        cancelled.outcome,
        Outcome::Rejected {
            reason: "cancelled".into()
        }
    );
    assert_eq!(db.push(context("alice"), request).await.unwrap(), cancelled);
    let view = db.view().await;
    assert_eq!((view.receipts, view.apply_calls, view.head.0), (1, 0, 0));
    assert!(view.values.is_empty() && view.jobs.is_empty());
}

#[tokio::test]
async fn application_rejection_rolls_back_data_and_jobs_but_keeps_the_receipt() {
    let db = store().await;
    db.fault(Fault::RejectAfterWrite).await;
    let request = set(db.epoch().await, "tea", "oolong");
    let rejected = db.push(context("alice"), request.clone()).await.unwrap();
    assert_eq!(
        rejected.outcome,
        Outcome::Rejected {
            reason: "demo_rejected".into()
        }
    );
    db.fault(Fault::None).await;
    assert_eq!(db.push(context("alice"), request).await.unwrap(), rejected);
    let view = db.view().await;
    assert!(view.values.is_empty() && view.jobs.is_empty() && view.events.is_empty());
    assert_eq!((view.apply_calls, view.receipts, view.head.0), (1, 1, 0));
}

#[tokio::test]
async fn error_or_panic_discards_the_entire_candidate_and_allows_same_op_retry() {
    for fault in [Fault::ErrorAfterWrite, Fault::PanicAfterWrite] {
        let db = store().await;
        db.fault(fault).await;
        let request = set(db.epoch().await, "tea", "oolong");
        assert_eq!(
            db.push(context("alice"), request.clone())
                .await
                .unwrap_err()
                .error,
            ErrorCode::InternalError
        );
        let view = db.view().await;
        assert!(view.values.is_empty() && view.jobs.is_empty() && view.events.is_empty());
        assert_eq!((view.apply_calls, view.receipts, view.head.0), (0, 0, 0));
        db.fault(Fault::None).await;
        assert!(matches!(
            db.push(context("alice"), request).await.unwrap().outcome,
            Outcome::Accepted { .. }
        ));
        assert_eq!(db.view().await.apply_calls, 1);
    }
}

#[tokio::test]
async fn owner_deadline_aborts_application_and_releases_lock_before_committing() {
    let db = store().await;
    db.fault(Fault::DelayAfterWrite(Duration::from_secs(1)))
        .await;
    let request = set(db.epoch().await, "tea", "oolong");
    let short_context = Context {
        auth_proof: None,
        subject: "alice".into(),
        deadline: Instant::now() + Duration::from_millis(50),
    };
    let started = Instant::now();
    assert_eq!(
        db.push(short_context, request.clone())
            .await
            .unwrap_err()
            .error,
        ErrorCode::InternalError
    );
    // Acquiring the owner mutex again proves release; allow broad scheduling
    // headroom while rejecting the original full one-second blocking delay.
    let view = db.view().await;
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(view.values.is_empty() && view.jobs.is_empty() && view.events.is_empty());
    assert_eq!((view.receipts, view.head.0), (0, 0));
    db.fault(Fault::None).await;
    assert!(matches!(
        db.push(context("alice"), request).await.unwrap().outcome,
        Outcome::Accepted { .. }
    ));
    assert_eq!(db.view().await.receipts, 1);
}

#[tokio::test]
async fn deterministic_application_size_violation_is_a_durable_rejection() {
    let db = store().await;
    let request = set(db.epoch().await, "too-large", &"x".repeat(512 * 1024));
    let first = db.push(context("alice"), request.clone()).await.unwrap();
    assert_eq!(
        first.outcome,
        Outcome::Rejected {
            reason: "size_limit".into()
        }
    );
    assert_eq!(db.push(context("alice"), request).await.unwrap(), first);
    let view = db.view().await;
    assert_eq!((view.receipts, view.head.0), (1, 0));
    assert!(view.values.is_empty() && view.jobs.is_empty());
}

#[tokio::test]
async fn own_canonical_event_and_foreign_metadata_preserve_the_same_event_order() {
    let db = store().await;
    let epoch = db.epoch().await;
    let request = set(epoch, "tea", " oolong ");
    let original = pending(&request);
    db.push(context("alice"), request).await.unwrap();
    db.server_write(
        context("alice"),
        STREAM.into(),
        epoch,
        message(
            Uuid::new_v4(),
            &Change::Set {
                key: "water".into(),
                value: "hot".into(),
            },
        ),
    )
    .await
    .unwrap();
    let own = db
        .pull(context("alice"), pull(epoch, 0, None, 100))
        .await
        .unwrap();
    let foreign = db
        .pull(context("bob"), pull(epoch, 0, None, 100))
        .await
        .unwrap();
    assert_eq!(own.events.len(), 2);
    assert_eq!(own.events[0].op, Some(original.op));
    assert_eq!(own.events[0].hash.as_deref(), Some(original.hash.as_str()));
    let EventValue::Change { message: canonical } = &own.events[0].value else {
        panic!("change required")
    };
    assert_ne!(canonical.id(), original.op);
    assert_eq!(
        decode(canonical).unwrap(),
        Change::Set {
            key: "tea".into(),
            value: "oolong".into()
        }
    );
    for (a, b) in own.events.iter().zip(&foreign.events) {
        assert_eq!((a.cursor, &a.value), (b.cursor, &b.value));
        assert!(b.op.is_none() && b.hash.is_none());
    }
    assert!(own.events[1].op.is_none() && own.events[1].hash.is_none());
    assert_eq!(db.view().await.receipts, 1);
}

#[tokio::test]
async fn noop_is_explicit_and_rolls_back_any_application_side_effects() {
    let db = store().await;
    db.fault(Fault::NoopAfterWrite).await;
    let result = db
        .push(context("alice"), set(db.epoch().await, "tea", "oolong"))
        .await
        .unwrap();
    assert_eq!(
        result.outcome,
        Outcome::Accepted {
            cursor: Position(1),
            result: Accepted::Noop {
                reason: "demo_noop".into()
            }
        }
    );
    let view = db.view().await;
    assert!(view.values.is_empty() && view.jobs.is_empty());
    assert!(
        matches!(&view.events[0].value, EventValue::Noop { reason, .. } if reason == "demo_noop")
    );
    let encoded = serde_json::to_value(&view.events[0].value).unwrap();
    assert!(encoded["message"].is_null());

    db.fault(Fault::None).await;
    let epoch = db.epoch().await;
    push_set(&db, epoch, "tea", "oolong").await;
    let unchanged = db
        .push(context("alice"), set(epoch, "tea", " oolong "))
        .await
        .unwrap();
    assert!(
        matches!(unchanged.outcome, Outcome::Accepted { result: Accepted::Noop { reason }, .. }
        if reason == "unchanged")
    );
    assert_eq!(db.view().await.jobs.len(), 1);
}

#[tokio::test]
async fn pull_target_stays_fixed_when_new_commits_arrive_between_pages() {
    let db = store().await;
    let epoch = db.epoch().await;
    push_set(&db, epoch, "a", "1").await;
    push_set(&db, epoch, "b", "2").await;
    let first = db
        .pull(context("alice"), pull(epoch, 0, None, 1))
        .await
        .unwrap();
    assert_eq!((first.cursor.0, first.to.0, first.more), (1, 2, true));
    push_set(&db, epoch, "c", "3").await;
    let last = db
        .pull(
            context("alice"),
            pull(epoch, first.cursor.0, Some(first.to.0), 1),
        )
        .await
        .unwrap();
    assert_eq!(
        (last.cursor.0, last.to.0, last.head.0, last.more),
        (2, 2, 3, false)
    );
    assert_eq!(last.events.len(), 1);
    let next = db
        .pull(context("alice"), pull(epoch, last.cursor.0, None, 100))
        .await
        .unwrap();
    assert_eq!((next.cursor.0, next.events.len()), (3, 1));
    assert_eq!(
        db.pull(context("alice"), pull(epoch, 0, Some(4), 1))
            .await
            .unwrap_err()
            .error,
        ErrorCode::CursorInvalid
    );
}

#[tokio::test]
async fn immutable_snapshot_pages_plus_tail_equal_an_independent_later_snapshot() {
    let db = store().await;
    let epoch = db.epoch().await;
    push_set(&db, epoch, "a", "1").await;
    push_set(&db, epoch, "b", "2").await;
    let request = snapshot(epoch, vec![], 1);
    let first = db
        .snapshot(context("alice"), request.clone())
        .await
        .unwrap();
    assert_eq!(first.cursor, Position(2));
    assert!(first.next.is_some());
    push_set(&db, epoch, "a", " 3 ").await;
    let delete = PushRequest {
        stream: STREAM.into(),
        epoch,
        operation: PushOperation::Message(message(
            Uuid::new_v4(),
            &Change::Delete { key: "b".into() },
        )),
    };
    db.push(context("alice"), delete).await.unwrap();
    push_set(&db, epoch, "c", "4").await;

    let mut base = BTreeMap::new();
    for item in &first.items {
        apply_canonical(&mut base, item).unwrap();
    }
    let mut token = first.next.clone();
    while let Some(next) = token {
        let part = db
            .snapshot(context("alice"), page(&request, first.snapshot, next))
            .await
            .unwrap();
        assert_eq!(part.cursor, first.cursor);
        for item in &part.items {
            apply_canonical(&mut base, item).unwrap();
        }
        token = part.next;
    }
    assert_eq!(
        base,
        BTreeMap::from([("a".into(), "1".into()), ("b".into(), "2".into())])
    );
    let mut cursor = first.cursor.0;
    let mut to = None;
    loop {
        let response = db
            .pull(context("alice"), pull(epoch, cursor, to, 1))
            .await
            .unwrap();
        for event in &response.events {
            if let EventValue::Change { message } = &event.value {
                apply_canonical(&mut base, message).unwrap();
            }
        }
        cursor = response.cursor.0;
        to = Some(response.to.0);
        if !response.more {
            break;
        }
    }
    let independent = db
        .snapshot(context("alice"), snapshot(epoch, vec![], 100))
        .await
        .unwrap();
    let mut other = BTreeMap::new();
    for item in &independent.items {
        apply_canonical(&mut other, item).unwrap();
    }
    assert_eq!(base, other);
    assert_eq!(base, db.view().await.values);
    assert_eq!(cursor, independent.cursor.0);
}

#[tokio::test]
async fn exhausted_snapshot_build_budget_is_retryable_and_publishes_no_image() {
    let db = store().await.with_snapshot_build_budget(Duration::ZERO);
    let request = snapshot(db.epoch().await, vec![], 100);
    let error = db
        .snapshot(context("alice"), request.clone())
        .await
        .unwrap_err();
    assert_eq!(error.error, ErrorCode::SnapshotBusy);
    assert_eq!(error.status(), 503);
    assert_eq!(error.retry_after, Some(1));
    assert_eq!(db.view().await.snapshots, 0);
    let db = db.with_snapshot_build_budget(Duration::from_secs(10));
    db.snapshot(context("alice"), request).await.unwrap();
    assert_eq!(db.view().await.snapshots, 1);
}

#[tokio::test]
async fn earlier_owner_timeout_is_not_mislabeled_as_snapshot_build_exhaustion() {
    let db = store().await.with_snapshot_build_budget(Duration::ZERO);
    let request = snapshot(db.epoch().await, vec![], 100);
    let expired = Context {
        auth_proof: None,
        subject: "alice".into(),
        deadline: Instant::now(),
    };
    let error = db.snapshot(expired, request).await.unwrap_err();
    assert_eq!(error.error, ErrorCode::InternalError);
    assert_eq!(error.status(), 500);
    assert_eq!(error.retry_after, None);
    assert_eq!(db.view().await.snapshots, 0);
}

#[tokio::test]
async fn lost_first_snapshot_response_repeats_before_quota_but_after_authorization() {
    let db = store().await;
    let request = snapshot(db.epoch().await, vec![], 100);
    let lost = db
        .snapshot(context("alice"), request.clone())
        .await
        .unwrap();
    db.limits(Limits {
        global_snapshots: 0,
        account_snapshots: 0,
        ..Limits::default()
    })
    .await;
    assert_eq!(
        db.snapshot(context("alice"), request.clone())
            .await
            .unwrap(),
        lost
    );
    let mut changed = request.clone();
    changed.kind = SnapshotKind::Create {
        limit: 1,
        pending: vec![],
    };
    assert_eq!(
        db.snapshot(context("alice"), changed)
            .await
            .unwrap_err()
            .error,
        ErrorCode::AttemptChanged
    );
    db.grant("alice", "account-a", false, false).await;
    assert_eq!(
        db.snapshot(context("alice"), request.clone())
            .await
            .unwrap_err()
            .error,
        ErrorCode::StreamForbidden
    );
    db.grant("alice", "account-a", true, false).await;
    assert_eq!(db.snapshot(context("alice"), request).await.unwrap(), lost);
    assert_eq!(db.view().await.snapshots, 1);
}

#[tokio::test]
async fn abandoned_snapshot_slot_expires_and_returns_a_bounded_retry_after() {
    let db = store().await;
    let original = snapshot(db.epoch().await, vec![], 100);
    let first = db
        .snapshot(context("alice"), original.clone())
        .await
        .unwrap();
    let next = SnapshotRequest {
        attempt: Uuid::new_v4(),
        ..original.clone()
    };
    let error = db
        .snapshot(context("alice"), next.clone())
        .await
        .unwrap_err();
    assert_eq!(error.error, ErrorCode::RateLimited);
    assert_eq!(error.retry_after, Some(600));
    db.advance_ms(SNAPSHOT_IDLE_MS);
    let second = db.snapshot(context("alice"), next).await.unwrap();
    assert_ne!(first.snapshot, second.snapshot);
    assert_eq!(db.view().await.snapshots, 1);
}

#[tokio::test]
async fn account_and_global_snapshot_budgets_cover_multiple_clients_and_subjects() {
    let db = store().await;
    let epoch = db.epoch().await;
    db.grant("bob", "account-a", true, true).await;
    db.grant("charlie", "account-b", true, true).await;
    db.grant("dana", "account-c", true, true).await;
    db.limits(Limits {
        account_snapshots: 1,
        global_snapshots: 2,
        ..Limits::default()
    })
    .await;
    db.snapshot(context("alice"), snapshot(epoch, vec![], 100))
        .await
        .unwrap();
    for subject in ["alice", "bob"] {
        assert_eq!(
            db.snapshot(context(subject), snapshot(epoch, vec![], 100))
                .await
                .unwrap_err()
                .error,
            ErrorCode::RateLimited
        );
    }
    db.snapshot(context("charlie"), snapshot(epoch, vec![], 100))
        .await
        .unwrap();
    assert_eq!(
        db.snapshot(context("dana"), snapshot(epoch, vec![], 100))
            .await
            .unwrap_err()
            .error,
        ErrorCode::RateLimited
    );
    assert_eq!(db.view().await.snapshots, 2);
}

#[tokio::test]
async fn snapshot_coverage_is_personalized_and_stays_immutable_after_unseen_executes() {
    let db = store().await;
    let epoch = db.epoch().await;
    let accepted = set(epoch, "accepted", "1");
    let rejected = set(epoch, "rejected", "2");
    let cancelled = set(epoch, "cancelled", "3");
    let unseen = set(epoch, "unseen", "4");
    let foreign = set(epoch, "foreign", "5");
    db.push(context("alice"), accepted.clone()).await.unwrap();
    db.fault(Fault::RejectAfterWrite).await;
    db.push(context("alice"), rejected.clone()).await.unwrap();
    db.fault(Fault::None).await;
    db.push(context("alice"), cancel(&cancelled)).await.unwrap();
    db.push(context("bob"), foreign.clone()).await.unwrap();
    let request = snapshot(
        epoch,
        [&accepted, &rejected, &cancelled, &unseen, &foreign]
            .into_iter()
            .map(pending)
            .collect(),
        100,
    );
    let cut = db
        .snapshot(context("alice"), request.clone())
        .await
        .unwrap();
    assert!(
        matches!(cut.ops[0].coverage, Coverage::Accepted { cursor, .. } if cursor <= cut.cursor)
    );
    assert_eq!(
        cut.ops[1].coverage,
        Coverage::Rejected {
            reason: "demo_rejected".into()
        }
    );
    assert_eq!(
        cut.ops[2].coverage,
        Coverage::Rejected {
            reason: "cancelled".into()
        }
    );
    assert_eq!(cut.ops[3].coverage, Coverage::Unseen);
    assert_eq!(cut.ops[4].coverage, Coverage::Mismatch);
    let later = db.push(context("alice"), unseen).await.unwrap();
    assert!(matches!(later.outcome, Outcome::Accepted { cursor, .. } if cursor > cut.cursor));
    assert_eq!(db.snapshot(context("alice"), request).await.unwrap(), cut);
}

#[tokio::test]
async fn a_lost_or_expired_image_rejects_valid_old_pages_without_confusing_tampering() {
    let db = store().await;
    let epoch = db.epoch().await;
    push_set(&db, epoch, "a", "1").await;
    push_set(&db, epoch, "b", "2").await;
    let create = snapshot(epoch, vec![], 1);
    let first = db.snapshot(context("alice"), create.clone()).await.unwrap();
    let continuation = page(&create, first.snapshot, first.next.unwrap());
    db.lose_snapshot_staging().await;
    assert_eq!(
        db.snapshot(context("alice"), continuation.clone())
            .await
            .unwrap_err()
            .error,
        ErrorCode::SnapshotExpired
    );
    let recreated = db.snapshot(context("alice"), create).await.unwrap();
    assert_ne!(recreated.snapshot, first.snapshot);
    assert_eq!(
        db.snapshot(context("alice"), continuation.clone())
            .await
            .unwrap_err()
            .error,
        ErrorCode::SnapshotExpired
    );
    assert_eq!(
        db.snapshot(context("bob"), continuation)
            .await
            .unwrap_err()
            .error,
        ErrorCode::BadRequest
    );
}

#[tokio::test]
async fn gc_retains_receipts_and_waits_thirty_days_after_first_observing_a_commit() {
    let db = store().await;
    let epoch = db.epoch().await;
    let request = set(epoch, "tea", "oolong");
    let accepted = db.push(context("alice"), request.clone()).await.unwrap();
    db.advance_ms(LOG_RETENTION_MS);
    db.gc().await; // First marking is conservative, even for an older event.
    assert_eq!(db.view().await.gc_floor, Position(0));
    db.advance_ms(LOG_RETENTION_MS - 1);
    db.gc().await;
    assert_eq!(db.view().await.gc_floor, Position(0));
    db.advance_ms(1);
    db.gc().await;
    let view = db.view().await;
    assert_eq!(
        (view.gc_floor.0, view.events.len(), view.receipts),
        (1, 0, 1)
    );
    assert_eq!(db.push(context("alice"), request).await.unwrap(), accepted);
    assert_eq!(
        db.pull(context("alice"), pull(epoch, 0, None, 100))
            .await
            .unwrap_err()
            .error,
        ErrorCode::CursorExpired
    );
    let image = db
        .snapshot(context("alice"), snapshot(epoch, vec![], 100))
        .await
        .unwrap();
    assert_eq!((image.cursor.0, image.items.len()), (1, 1));
}

#[tokio::test]
async fn epoch_change_fences_old_requests_and_snapshots_before_disclosing_cache_contents() {
    let db = store().await;
    let old_owner = db.clone();
    let epoch = db.epoch().await;
    let push = set(epoch, "tea", "oolong");
    db.push(context("alice"), push.clone()).await.unwrap();
    let create = snapshot(epoch, vec![pending(&push)], 100);
    db.snapshot(context("alice"), create.clone()).await.unwrap();
    let next_epoch = db.new_epoch().await;
    assert_ne!(next_epoch, epoch);
    for error in [
        old_owner
            .push(context("alice"), push.clone())
            .await
            .unwrap_err(),
        old_owner
            .pull(context("alice"), pull(epoch, 0, None, 100))
            .await
            .unwrap_err(),
        old_owner
            .snapshot(context("alice"), create)
            .await
            .unwrap_err(),
    ] {
        assert_eq!(error.error, ErrorCode::EpochChanged);
        assert_eq!(error.epoch, Some(next_epoch));
    }
    db.grant("alice", "account-a", false, false).await;
    let denied = db.push(context("alice"), push).await.unwrap_err();
    assert_eq!(denied.error, ErrorCode::StreamForbidden);
    assert!(denied.epoch.is_none());
    assert!(db.view().await.values.is_empty());
    assert_ne!(MemoryBackend::new(STREAM).epoch().await, next_epoch);
}
