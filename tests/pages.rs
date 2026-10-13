use base64::{Engine as _, engine::general_purpose::STANDARD};
use replicaman_server::{ErrorCode, Result, backend::Context, engine::*, wire::*};
use serde_json::json;
use std::time::{Duration, Instant};
use uuid::Uuid;

struct Reader {
    epoch: Uuid,
    head: Position,
    floor: Position,
    log: Vec<StoredEvent>,
}
impl ReadTransaction for Reader {
    fn state(&mut self) -> Result<StreamState> {
        Ok(StreamState {
            epoch: self.epoch,
            head: self.head,
            gc_floor: self.floor,
        })
    }
    fn access(&mut self, _: &str) -> Result<Access> {
        Ok(Access {
            read: true,
            write: false,
        })
    }
    fn events(
        &mut self,
        after: Position,
        through: Position,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        Ok(self
            .log
            .iter()
            .filter(|e| e.cursor > after && e.cursor <= through)
            .take(limit)
            .cloned()
            .collect())
    }
    fn receipt(&mut self, _: Uuid) -> Result<Option<StoredReceipt>> {
        Ok(None)
    }
}
fn event(cursor: u64, bytes: usize) -> StoredEvent {
    StoredEvent { cursor: Position(cursor), author: None, op: None, hash: None,
        value: EventValue::Change { message: CloudEvent::from_value(json!({"specversion":"1.0",
            "id":Uuid::new_v4(), "source":"urn:test", "type":"test.event.v1",
            "datacontenttype":"application/octet-stream", "data_base64":STANDARD.encode(vec![1; bytes])})).unwrap() } }
}
fn context() -> Context {
    Context {
        auth_proof: None,
        subject: "alice".into(),
        deadline: Instant::now() + Duration::from_secs(30),
    }
}
fn request(tx: &Reader, cursor: u64, to: Option<u64>, limit: usize) -> PullRequest {
    PullRequest {
        stream: "catalog".into(),
        epoch: tx.epoch,
        cursor: Position(cursor),
        to: to.map(Position),
        limit,
    }
}

#[test]
fn byte_budget_and_count_limit_never_skip_an_event() {
    let mut tx = Reader {
        epoch: Uuid::new_v4(),
        head: Position(40),
        floor: Position(0),
        log: vec![
            event(2, 400_000),
            event(9, 400_000),
            event(13, 900_000),
            event(39, 0),
        ],
    };
    let mut request = request(&tx, 0, None, 100);
    let mut received = vec![];
    loop {
        let response = pull(&mut tx, &context(), &request).unwrap();
        assert_eq!(response.to, Position(40));
        assert_eq!(response.events.len(), 1);
        let size = json_size(&response).unwrap();
        assert!(size <= MAX_BODY_BYTES);
        if response.events[0].cursor == Position(13) {
            assert!(size > PAGE_BYTES);
        } else {
            assert!(size <= PAGE_BYTES);
        }
        received.extend(response.events.iter().map(|e| e.cursor.0));
        if !response.more {
            assert_eq!(response.cursor, Position(40));
            break;
        }
        assert_eq!(response.cursor, response.events.last().unwrap().cursor);
        request.cursor = response.cursor;
        request.to = Some(response.to);
        request.limit = 1;
    }
    assert_eq!(received, vec![2, 9, 13, 39]);
}

#[test]
fn empty_ranges_advance_to_target_and_new_writes_do_not_move_it() {
    let mut tx = Reader {
        epoch: Uuid::new_v4(),
        head: Position(10),
        floor: Position(0),
        log: vec![event(5, 1)],
    };
    let initial = request(&tx, 0, None, 1);
    let response = pull(&mut tx, &context(), &initial).unwrap();
    assert!(!response.more);
    assert_eq!(response.cursor, Position(10));
    tx.head = Position(20);
    tx.log.push(event(15, 1));
    let fixed = request(&tx, 10, Some(10), 100);
    let response = pull(&mut tx, &context(), &fixed).unwrap();
    assert_eq!(response.head, Position(20));
    assert_eq!(response.to, Position(10));
    assert!(response.events.is_empty() && !response.more);
    let future = request(&tx, 10, Some(30), 100);
    assert_eq!(
        pull(&mut tx, &context(), &future).unwrap_err().error,
        ErrorCode::CursorInvalid
    );
    tx.floor = Position(11);
    assert_eq!(
        pull(&mut tx, &context(), &fixed).unwrap_err().error,
        ErrorCode::CursorExpired
    );
}

#[test]
fn malformed_storage_order_is_an_error_not_a_partial_success() {
    let mut tx = Reader {
        epoch: Uuid::new_v4(),
        head: Position(10),
        floor: Position(0),
        log: vec![event(5, 1), event(4, 1)],
    };
    let req = request(&tx, 0, None, 100);
    assert_eq!(
        pull(&mut tx, &context(), &req).unwrap_err().error,
        ErrorCode::InternalError
    );
}
