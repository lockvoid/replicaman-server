use base64::{Engine as _, engine::general_purpose::STANDARD};
use replicaman_server::{ErrorCode, backend::Context, engine::*, snapshot::*, wire::*};
use serde_json::json;
use std::time::{Duration, Instant};
use uuid::Uuid;

fn context(subject: &str) -> Context {
    Context {
        auth_proof: None,
        subject: subject.into(),
        deadline: Instant::now() + Duration::from_secs(30),
    }
}
fn event(bytes: usize) -> CloudEvent {
    CloudEvent::from_value(json!({"specversion":"1.0", "id":Uuid::new_v4(),
        "source":"urn:test", "type":"test.item.v1", "datacontenttype":"application/octet-stream",
        "data_base64":STANDARD.encode(vec![42; bytes])}))
    .unwrap()
}
fn request() -> SnapshotRequest {
    SnapshotRequest {
        stream: "catalog".into(),
        epoch: Uuid::new_v4(),
        client: Uuid::new_v4(),
        attempt: Uuid::new_v4(),
        kind: SnapshotKind::Create {
            limit: 2,
            pending: vec![],
        },
    }
}
fn build(
    req: &SnapshotRequest,
    items: Vec<CloudEvent>,
    key: &TokenKey,
    limits: SnapshotLimits,
) -> Snapshot {
    Snapshot::new(
        &context("alice"),
        req,
        SnapshotCut {
            epoch: req.epoch,
            cursor: Position(10),
            at_ms: 0,
            items,
        },
        vec![],
        0,
        limits,
        key,
    )
    .unwrap()
}
fn continuation(req: &SnapshotRequest, page: &SnapshotResponse) -> SnapshotRequest {
    SnapshotRequest {
        kind: SnapshotKind::Page {
            snapshot: page.snapshot,
            page: page.next.clone().unwrap(),
            limit: None,
        },
        ..req.clone()
    }
}

#[test]
fn pages_are_immutable_and_bound_to_the_entire_context() {
    let req = request();
    let key = TokenKey::random();
    let items = vec![event(10), event(11), event(12)];
    let original_id = items[0].id();
    let mut snapshot = build(&req, items, &key, SnapshotLimits::default());
    let first = snapshot.page(&context("alice"), &req, 0).unwrap();
    assert_eq!(first.items.len(), 2);
    assert_ne!(first.items[0].id(), original_id);
    assert_eq!(first, snapshot.page(&context("alice"), &req, 1000).unwrap());
    let next = continuation(&req, &first);
    assert_eq!(key.verify(&context("alice"), &next).unwrap(), Some(1));
    let last = snapshot.page(&context("alice"), &next, 1000).unwrap();
    assert_eq!(last.items.len(), 1);
    assert_eq!(last.next, None);
    assert_eq!(last, snapshot.page(&context("alice"), &next, 2000).unwrap());

    for field in 0..5 {
        let mut foreign = next.clone();
        match field {
            0 => foreign.client = Uuid::new_v4(),
            1 => foreign.attempt = Uuid::new_v4(),
            2 => foreign.epoch = Uuid::new_v4(),
            3 => foreign.stream = "different".into(),
            _ => {
                if let SnapshotKind::Page { snapshot, .. } = &mut foreign.kind {
                    *snapshot = Uuid::new_v4();
                }
            }
        }
        assert_eq!(
            key.verify(&context("alice"), &foreign).unwrap_err().error,
            ErrorCode::BadRequest
        );
    }
    assert_eq!(
        key.verify(&context("bob"), &next).unwrap_err().error,
        ErrorCode::BadRequest
    );
    let mut forged = next.clone();
    if let SnapshotKind::Page { page, .. } = &mut forged.kind {
        page.replace_range(10..11, "!");
    }
    assert_eq!(
        key.verify(&context("alice"), &forged).unwrap_err().error,
        ErrorCode::BadRequest
    );
    // Destroying the image does not destroy the ability to recognize a valid
    // expired/lost page. The owner can return snapshot_expired, not bad_request.
    drop(snapshot);
    assert_eq!(key.verify(&context("alice"), &next).unwrap(), Some(1));
}

#[test]
fn changed_attempt_parameters_are_rejected_without_renewal() {
    let req = request();
    let mut snapshot = build(
        &req,
        vec![event(1); 3],
        &TokenKey::random(),
        SnapshotLimits::default(),
    );
    let first = snapshot.page(&context("alice"), &req, 0).unwrap();
    let mut altered = req.clone();
    altered.kind = SnapshotKind::Create {
        limit: 1,
        pending: vec![],
    };
    assert_eq!(
        snapshot
            .page(&context("alice"), &altered, 1000)
            .unwrap_err()
            .error,
        ErrorCode::AttemptChanged
    );
    let mut altered_page = continuation(&req, &first);
    if let SnapshotKind::Page { limit, .. } = &mut altered_page.kind {
        *limit = Some(1);
    }
    assert_eq!(
        snapshot
            .page(&context("alice"), &altered_page, 2000)
            .unwrap_err()
            .error,
        ErrorCode::AttemptChanged
    );
    assert_eq!(snapshot.retry_after(2000), 598);
    altered.kind = SnapshotKind::Create {
        limit: 2,
        pending: vec![Pending {
            op: Uuid::new_v4(),
            hash: "a".repeat(64),
        }],
    };
    assert_eq!(
        snapshot
            .page(&context("alice"), &altered, 2000)
            .unwrap_err()
            .error,
        ErrorCode::AttemptChanged
    );
}

#[test]
fn ttl_rounds_down_and_expired_images_cannot_be_revived() {
    let req = request();
    let key = TokenKey::random();
    let mut expired = build(&req, vec![], &key, SnapshotLimits::default());
    assert!(!expired.expired(SNAPSHOT_IDLE_MS - 1000));
    assert!(expired.expired(SNAPSHOT_IDLE_MS - 750));
    assert_eq!(
        expired
            .page(&context("alice"), &req, SNAPSHOT_IDLE_MS - 750)
            .unwrap_err()
            .error,
        ErrorCode::SnapshotExpired
    );
    assert_eq!(expired.retry_after(SNAPSHOT_IDLE_MS - 750), 1);

    let now = SNAPSHOT_LIFETIME_MS - 1500;
    let mut near_hard_limit = Snapshot::new(
        &context("alice"),
        &req,
        SnapshotCut {
            epoch: req.epoch,
            cursor: Position(0),
            at_ms: 0,
            items: vec![],
        },
        vec![],
        now,
        SnapshotLimits::default(),
        &key,
    )
    .unwrap();
    let response = near_hard_limit.page(&context("alice"), &req, now).unwrap();
    assert_eq!(response.ttl, 1);
    assert_eq!(response.next, None);
    assert_eq!(
        near_hard_limit
            .page(&context("alice"), &req, now + 750)
            .unwrap_err()
            .error,
        ErrorCode::SnapshotExpired
    );
}

#[test]
fn byte_budget_splits_pages_and_preserves_an_oversized_item() {
    let mut req = request();
    req.kind = SnapshotKind::Create {
        limit: 100,
        pending: vec![],
    };
    let key = TokenKey::random();
    let mut snapshot = build(
        &req,
        vec![event(400_000), event(400_000), event(900_000), event(1)],
        &key,
        SnapshotLimits {
            max_item_bytes: 1_000_000,
            ..SnapshotLimits::default()
        },
    );
    let mut next = req.clone();
    let mut sizes = vec![];
    loop {
        let page = snapshot.page(&context("alice"), &next, 0).unwrap();
        assert_eq!(page.items.len(), 1);
        let size = json_size(&page).unwrap();
        assert!(size <= MAX_BODY_BYTES);
        sizes.push(size);
        if page.next.is_none() {
            break;
        }
        next = continuation(&req, &page);
    }
    assert_eq!(sizes.len(), 4);
    assert!(sizes[0] <= PAGE_BYTES && sizes[1] <= PAGE_BYTES);
    assert!(sizes[2] > PAGE_BYTES);
    assert!(sizes[3] <= PAGE_BYTES);
}

#[test]
fn pending_receipts_stay_immutable_even_if_unknown_operation_executes_later() {
    let mut req = request();
    let pending: Vec<_> = (0..4)
        .map(|_| Pending {
            op: Uuid::new_v4(),
            hash: "a".repeat(64),
        })
        .collect();
    req.kind = SnapshotKind::Create {
        limit: 1,
        pending: pending.clone(),
    };
    let outcomes = [
        Coverage::Accepted {
            cursor: Position(9),
            result: Accepted::Applied,
        },
        Coverage::Accepted {
            cursor: Position(11),
            result: Accepted::Noop {
                reason: "already_done".into(),
            },
        },
        Coverage::Unseen,
        Coverage::Mismatch,
    ];
    let ops: Vec<_> = pending
        .into_iter()
        .zip(outcomes)
        .map(|(p, coverage)| SnapshotOp {
            op: p.op,
            hash: p.hash,
            coverage,
        })
        .collect();
    let mut snapshot = Snapshot::new(
        &context("alice"),
        &req,
        SnapshotCut {
            epoch: req.epoch,
            cursor: Position(10),
            at_ms: 0,
            items: vec![event(1), event(1)],
        },
        ops.clone(),
        0,
        SnapshotLimits::default(),
        &TokenKey::random(),
    )
    .unwrap();
    let first = snapshot.page(&context("alice"), &req, 0).unwrap();
    assert_eq!(first.ops, ops);
    let second = snapshot
        .page(&context("alice"), &continuation(&req, &first), 1000)
        .unwrap();
    assert_eq!(second.ops, ops);
    assert_eq!(second.cursor, Position(10));
}

#[test]
fn construction_and_owner_clock_are_bounded() {
    let req = request();
    let key = TokenKey::random();
    let error = Snapshot::new(
        &context("alice"),
        &req,
        SnapshotCut {
            epoch: req.epoch,
            cursor: Position(0),
            at_ms: 0,
            items: vec![event(4)],
        },
        vec![],
        0,
        SnapshotLimits {
            max_data_bytes: 3,
            ..SnapshotLimits::default()
        },
        &key,
    )
    .err()
    .unwrap();
    assert_eq!(error.error, ErrorCode::StoragePressure);
    let mut snapshot = build(&req, vec![], &key, SnapshotLimits::default());
    snapshot.page(&context("alice"), &req, 2000).unwrap();
    assert_eq!(
        snapshot
            .page(&context("alice"), &req, 1999)
            .unwrap_err()
            .error,
        ErrorCode::InternalError
    );
    assert_eq!(snapshot.retry_after(2000), 600);
}

fn pages_of(snapshot: &mut Snapshot, req: &SnapshotRequest) -> Vec<Vec<CloudEvent>> {
    let mut next = req.clone();
    let mut pages = vec![];
    loop {
        let page = snapshot.page(&context("alice"), &next, 0).unwrap();
        pages.push(page.items.clone());
        if page.next.is_none() {
            return pages;
        }
        next = continuation(req, &page);
    }
}

fn pack(items: Vec<CloudEvent>, limit: usize, envelope: usize) -> Vec<Vec<CloudEvent>> {
    let limits = SnapshotLimits {
        max_item_bytes: 1_000_000,
        ..SnapshotLimits::default()
    };
    let mut pager = SnapshotPager::new(limit, limits, envelope).unwrap();
    let mut pages = vec![];
    for item in items {
        pages.extend(pager.push(item).unwrap());
    }
    pages.push(pager.finish());
    pages
}

fn data(pages: &[Vec<CloudEvent>]) -> Vec<Vec<Vec<u8>>> {
    pages
        .iter()
        .map(|page| page.iter().map(|item| item.data().unwrap()).collect())
        .collect()
}

#[test]
fn the_pager_packs_the_same_pages_as_a_whole_snapshot() {
    let mut req = request();
    req.kind = SnapshotKind::Create {
        limit: 3,
        pending: vec![],
    };
    let items: Vec<_> = [300_000, 300_000, 300_000, 1, 2, 3, 4, 900_000, 5]
        .into_iter()
        .map(event)
        .collect();
    let mut snapshot = build(
        &req,
        items.clone(),
        &TokenKey::random(),
        SnapshotLimits {
            max_item_bytes: 1_000_000,
            ..SnapshotLimits::default()
        },
    );
    let envelope = json_size(&SnapshotResponse {
        stream: req.stream.clone(),
        epoch: req.epoch,
        client: req.client,
        attempt: req.attempt,
        snapshot: Uuid::new_v4(),
        cursor: Position(10),
        items: vec![],
        next: Some("A".repeat(TOKEN_LEN)),
        ttl: 600,
        ops: vec![],
    })
    .unwrap();
    let whole = pages_of(&mut snapshot, &req);
    let streamed = pack(items, 3, envelope);
    assert_eq!(data(&streamed), data(&whole));
    let fresh: Vec<_> = streamed.iter().flatten().map(CloudEvent::id).collect();
    assert_eq!(
        fresh.iter().collect::<std::collections::HashSet<_>>().len(),
        fresh.len()
    );
}

#[test]
fn reserved_pages_fit_any_attempt_with_the_largest_outcomes() {
    let stream = "catalog/1234567890";
    let reserve = envelope_reserve(stream).unwrap();
    // Items much smaller than the outcomes' size fill each page to within one
    // item of PAGE_BYTES, so a reserve without `ops` would overflow below.
    let pages = pack(
        (0..300).map(|index| event(9_000 + index)).collect(),
        100,
        reserve,
    );
    assert!(pages.len() > 1);
    assert!(pages.iter().all(|page| page.len() < 100));
    let ops: Vec<_> = (0..MAX_PENDING)
        .map(|_| SnapshotOp {
            op: Uuid::new_v4(),
            hash: "f".repeat(64),
            coverage: Coverage::Accepted {
                cursor: Position(i64::MAX as u64),
                result: Accepted::Noop {
                    reason: "z".repeat(64),
                },
            },
        })
        .collect();
    let key = TokenKey::random();
    let req = SnapshotRequest {
        stream: stream.into(),
        ..request()
    };
    for (index, items) in pages.iter().enumerate() {
        let response = SnapshotResponse {
            stream: stream.into(),
            epoch: req.epoch,
            client: req.client,
            attempt: req.attempt,
            snapshot: Uuid::new_v4(),
            cursor: Position(i64::MAX as u64),
            items: items.clone(),
            next: Some(
                key.page_token(&context("alice"), &req, Uuid::new_v4(), index + 1)
                    .unwrap(),
            ),
            ttl: u16::MAX,
            ops: ops.clone(),
        };
        assert!(json_size(&response).unwrap() <= PAGE_BYTES);
    }
}

#[test]
fn page_tokens_have_one_width_and_verify_for_their_index() {
    let req = request();
    let key = TokenKey::random();
    let id = Uuid::new_v4();
    for index in [0, 1, 511, u32::MAX as usize] {
        let token = key.page_token(&context("alice"), &req, id, index).unwrap();
        assert_eq!(token.len(), TOKEN_LEN);
        let page = SnapshotRequest {
            kind: SnapshotKind::Page {
                snapshot: id,
                page: token,
                limit: None,
            },
            ..req.clone()
        };
        assert_eq!(key.verify(&context("alice"), &page).unwrap(), Some(index));
    }
}

#[test]
fn an_empty_cut_is_one_empty_page_and_limits_hold_while_streaming() {
    let limits = SnapshotLimits {
        max_items: 2,
        max_data_bytes: 10,
        max_item_bytes: 6,
    };
    assert!(
        SnapshotPager::new(1, limits, 100)
            .unwrap()
            .finish()
            .is_empty()
    );
    let mut pager = SnapshotPager::new(1, limits, 100).unwrap();
    assert!(pager.push(event(6)).unwrap().is_none());
    assert_eq!(pager.push(event(4)).unwrap().unwrap().len(), 1);
    let error = pager.push(event(1)).unwrap_err();
    assert_eq!(error.error, ErrorCode::StoragePressure);
    let mut large = SnapshotPager::new(1, limits, 100).unwrap();
    assert_eq!(
        large.push(event(7)).unwrap_err().error,
        ErrorCode::StoragePressure
    );
    assert!(SnapshotPager::new(0, limits, 100).is_err());
    assert!(SnapshotPager::new(1, limits, MAX_BODY_BYTES).is_err());
}

#[test]
fn old_signed_page_cannot_read_a_recreated_attempt() {
    let req = request();
    let key = TokenKey::random();
    let mut old = build(&req, vec![event(1); 3], &key, SnapshotLimits::default());
    let first = old.page(&context("alice"), &req, 0).unwrap();
    let late = continuation(&req, &first);
    assert_eq!(key.verify(&context("alice"), &late).unwrap(), Some(1));
    let mut replacement = build(&req, vec![event(2); 3], &key, SnapshotLimits::default());
    assert_eq!(
        replacement
            .page(&context("alice"), &late, 0)
            .unwrap_err()
            .error,
        ErrorCode::SnapshotExpired
    );
}
