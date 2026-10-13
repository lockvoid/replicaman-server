# Replicaman protocol v1

Replicaman delivers changes; the application defines their meaning and owns storage through transactional callbacks. It has no dependency on a database, ORM, or application document format. Messages use a CloudEvents profile with application-defined payloads.

Read [WIRE.md](./WIRE.md) for exact fields, [IMPLEMENTATION.md](./IMPLEMENTATION.md) for transaction boundaries, and [formal/README.md](../formal/README.md) for verification and its limits.

## Data model

- An **operation** is a client's proposed change.
- An **event** is the result committed by the server.
- A **snapshot** is the complete confirmed state at log position S, including metadata needed to apply subsequent events.
- A **stream** groups data with shared access rights and event order, such as a list, its tasks, and their attachments. There is no ordering or atomic application across streams.

The central invariant is **snapshot at S + events after S through T ≡ snapshot at T**. Local unconfirmed edits are tracked separately. Every event must be fully applicable to the preceding state, without dependencies on future events. Every atomic change within a stream occupies one log entry, regardless of its writer.

Read access includes **all retained stream history**, including history embedded in documents. An application granting access only to current data must create a separate, cleaned access scope. Filtering individual events by reader is forbidden.

## Push: propose a change

`POST /replicaman/v1/push` accepts a message whose `id` identifies the operation (`op`).

- `accepted`: data, one event, and a receipt commit atomically. The response includes the acceptance position; it **does not advance the client's read cursor**. Server normalization may make the event differ from the request.
- `rejected`: only a receipt is saved; no data changes or external jobs are created.
- An accepted operation with no data change is an explicit `noop` with a reason. A temporary failure is not a rejection.

A retry of `(stream, epoch, op)` returns the same result. A different author or message hash produces `operation_changed`. The author is a stable authorization subject, not an access token. Read access is sufficient to retrieve an existing receipt. Receipts last for the entire `epoch`: no TTL or routine epoch rotation for cleanup.

**Cancellation** uses the same endpoint with `cancel: {op, hash}`. The server atomically returns an existing receipt or saves `rejected/cancelled`, preventing a delayed original request from executing. If acceptance wins the race, cancellation returns `accepted` without undoing it. The author needs only read access.

## Pull: read committed results

`POST /replicaman/v1/pull` accepts a cursor and an optional target `to`. Without a target, the server captures the current `head`. The response contains events, the next cursor, `to`, the observed `head`, and `more`.

Subsequent pages keep the same `to`, so new writes cannot extend the traversal. Numeric gaps are allowed; missing events are not. The last page advances the cursor to `to`, including across an empty interval. Positions follow commit order: a later event cannot become visible before an earlier one.

**Apply the client's own event to the confirmed base too.** Its `op` and original request hash also establish acceptance; reading does not require a separate receipt request. One transaction saves the base, receipt, removal of covered optimistic edits, reconciled queue, and cursor. A `noop` passes its reason to the application.

## Snapshot: establish a complete base

`POST /replicaman/v1/snapshot` starts a snapshot using `client`, an `attempt` persisted before network access, `page: null`, and hashes of prepared operations. A retry returns the same active snapshot **before checking the quota for a new one**.

The response identifies the `snapshot`, position S, items, next page, lifetime, and operation outcomes. Pages are immutable, with boundaries fixed at creation. Never combine different snapshot IDs. Repeating creation after expiry may return a new content ID.

For each prepared operation, the server returns a receipt, `unseen`, or an identity mismatch:

| Outcome | Client action |
| --- | --- |
| Accepted at P ≤ S | Its result is already in the snapshot; remove the optimistic edit. |
| Accepted at P > S | Keep the edit until the log delivers its result. |
| `unseen` | The operation is absent from S but may execute later; this is **not a rejection**. |
| Author/hash mismatch | Isolate the disputed work for recovery. |

**An unknown outcome or revoked write access must not block reading.** Stage pages separately. The final callback atomically replaces the base, reconciles the current queue, and sets the cursor to S. Preserve edits that cannot be reconciled in a recovery archive, then read the log after S.

During assembly, clients may save new local intents but must not prepare new operations. Persist this barrier and the fixed pending list atomically; both survive restart. Never resume an abandoned attempt. A snapshot expires after 600 seconds without a page read, or 24 hours after its cut, whichever comes first. The log retains events for **at least 30 days after commit**, so transition from a live snapshot to the log requires no separate tail lease.

## Client recovery

Save each local edit with its outbound intent. Before network access, fix the operation ID and message bytes permanently. One sender per stream prepares the next operation after applying the previous canonical result and reconciling dependent intents. Local editing continues during network requests.

Callbacks check the local session, expected cursor, and current edit version. Data, queue, and cursor must never commit separately.

| Condition | Action |
| --- | --- |
| Disconnect, 429, or 5xx | Retry automatically with backoff and `Retry-After`; notify the application after 8 consecutive failures and keep retrying. |
| Sending is paused | Continue reading; after repair, retry the same bytes or request cancellation. |
| Expired cursor | Obtain a snapshot with prepared-operation coverage. |
| Inapplicable event or damaged base | Preserve local work and attempt one snapshot recovery; expose a repeated failure. |
| Unsupported format or storage failure | Pause until repaired; acknowledge nothing. |
| Reply from an old session or superseded request | Discard it and continue. An invalid reply to the current request requires repair. |
| Account change | Isolate the previous account's data and queue. |
| Epoch change | Reload catalogs. Archive editable data before loading the new base; never migrate the old queue automatically. |

Failure counts never turn an unknown outcome into rejection. Resume preserves the operation ID. A new ID is allowed after a known rejection/cancellation or for a new intent.

## Server and application responsibilities

The host creates a stream and its first epoch before publishing its address. Its bootstrap API returns accessible `{stream, epoch}` pairs, including after reinstall. Every request uses a concrete epoch. Until a stream exists, local edits remain intents. `404 stream_not_found` refreshes discovery; `410 stream_gone` closes the stream while preserving local work. Deleted addresses are never reused.

All writers, including other APIs and background jobs, atomically commit data and the log. With RPC, the data owner executes the transaction. External jobs are saved in that transaction; their execution requires idempotency or recipient deduplication.

Cursor validity and epoch are determined from authoritative state. A lagging node waits or returns `503 not_caught_up`. Confirmed history and receipts are immutable within an epoch. Any rollback, including failover that loses a committed write, requires a new epoch. Fence the previous owner from reads and writes before the new owner serves requests.

A doorbell is only a hint to check a stream. Also check on launch, foregrounding, network recovery, and periodically while the application runs.

The [resource profile](./IMPLEMENTATION.md#resource-profile) is mandatory: storage pressure limits new writes rather than shortening promised retention. The initial resource profile targets small record streams; other application profiles require separate validation.
