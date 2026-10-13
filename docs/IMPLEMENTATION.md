# Replicaman implementation and verification

This document supplements the [protocol](./PROTOCOL.md) and [wire format](./WIRE.md). The transaction boundaries apply to every database. PostgreSQL is an implementation example, not a library dependency. The release scenarios below are acceptance requirements; the [server guide](./SERVER.md#verification-scope) describes current test coverage.

## Client state and atomic callbacks

The application stores a confirmed base, local intents, prepared operations, and recovery copies. Intentions may be reconciled and encoded again; a prepared operation's `op`, message, and hash are immutable. Its outcome is unknown, accepted at position P, or rejected. An accepted local edit remains over the base until the log or a snapshot covers P. A late receipt must not put an already covered edit back in the queue.

A client without a confirmed local base starts with a snapshot. Cursor `0` alone does not establish that an empty local database matches the stream's initial state.

The callback names below are illustrative; each row defines one required atomic commit.

| Callback | Commit together |
| --- | --- |
| `saveLocal` | Local edit, send intent, and new local version |
| `preparePush` | Check that recovery is inactive, reconcile the first intent, and save immutable `op`, message, and hash before network access |
| `recordVerdict` | Receipt and queue state; leave the read cursor unchanged |
| `applyPage` | Confirmed events and their receipts, removal of covered local edits, reconciled intents, and cursor |
| `beginSnapshot` | Durable barrier against preparing operations, new `attempt`, fixed complete `pending`, and recovery context |
| `installSnapshot` | New base, operation check results, reconciliation of the current queue, and cursor S |
| `archiveLocal` | Original documents, intents, operations, and recovery context before detaching them from the new base |

Callbacks check the local session, expected cursor, and local edit version. Computation may happen outside the transaction, but its result may commit only if the versions still match; otherwise, recompute it. Every queue change also increments the version. An account change, epoch change, or replacement of unfinished recovery creates a new local session. Ignore responses from older sessions and superseded requests, then continue from persisted state.

Failure to reconcile a local edit need not stop confirmed reads. In the **same transaction**, the callback preserves the conflicting branch for recovery and installs the confirmed result; sending its dependent branch remains paused. If the recovery copy cannot be saved, installation does not commit. Unprepared intents must not disappear because the snapshot lacks their records.

### Sending and retries

Each stream has one sender and at most one prepared operation awaiting reconciliation. Prepare the next intent only after applying the previous canonical result or handling its rejection. An operation may combine several application changes before its bytes become immutable. Two network exchanges for a dependent operation are a deliberate v1 simplification; there is no general batch or send pipeline. Independent streams run concurrently within application limits.

The server exposes an event's `op/hash` only to its author; the hash identifies the original request. A match with a prepared operation confirms `accepted` at the event's position even if the push response was lost. `applyPage` saves the receipt, applies the canonical result, and removes the local edit atomically. Reads must not require an extra push to resolve the outcome. On a hash mismatch, archive the disputed work and continue confirmed reads.

After a network failure, use exponential backoff with jitter. Cap the client's own delay at 60 seconds, but honor a longer `Retry-After`. Eight consecutive failures of one kind of work produce a visible status and notify the application; **automatic retries continue**. While offline, wait for connectivity. Network restoration, startup, and foreground entry automatically restart the cycle, respecting `Retry-After`. Success resets that work's counter; successful reads do not reset send failures.

Durable `paused` is reserved for an explicit application pause or an access, format, state, or storage error that requires repair. It includes a reason and a `resume` action. Acknowledge nothing until the data problem is fixed. Reading and sending remain independent.

To resolve an irreparable prepared operation, cancel it with its original hash. After `rejected/cancelled`, the application may recover the intent as a new operation. After `accepted`, apply the result first; undoing it requires a new application action. A cancellation timeout leaves the outcome unknown: repeat the same cancellation. A failure count never authorizes deleting an operation or assigning it a new ID.

The application callback returns deterministic domain invalidity as `rejected`. An unexplained exception rolls back the transaction and remains a 5xx, even after repeated identical failures. Cancellation bypasses this callback and can resolve a persistently failing operation if the server can commit receipts.

### Installing a snapshot

Before network access, `beginSnapshot` atomically persists the recovery phase and an `attempt` containing every prepared operation not yet covered. `preparePush` checks this phase inside its transaction: preparation computed earlier cannot commit after the barrier. After a crash, restore the phase **before starting the sender**. Log page application is also suspended. Retries and cancellation of an already prepared `op` remain allowed; local editing continues to save new intents.

Snapshot installation removes the barrier atomically with the new base and cursor. Abandoning a build atomically removes the barrier and invalidates its local session, `attempt`, and pages; ignore late responses. Never resume that attempt. A new recovery captures the current queue with a new `attempt`. The SDK stops requesting the abandoned attempt; its server slot expires, and the new attempt automatically waits according to `Retry-After`. This prevents an unlisted operation from entering the snapshot and then being applied again as a local edit.

Assemble pages separately by `(local session, snapshot)`. A new content identifier starts a new assembly. The SDK serializes creation and page retries, ignoring late responses to superseded requests. The final page must not cause a partial installation.

At installation, reconcile each prepared operation's saved receipt with the snapshot's `ops`:

- Accepted at P ≤ S: remove the local edit; the base already contains its result.
- Accepted at P > S: retain the edit until the log reaches P.
- Rejected: preserve the work for application recovery and reconcile dependent intents.
- `unseen`: retain the same bytes and unknown outcome; reads remain allowed. It must not overwrite a newer known receipt.
- `mismatch`: archive the disputed operation; do not assign a new ID automatically.

An `unseen` result contradicting a reliable acceptance receipt at P ≤ S is a server contract violation. Acceptance after snapshot publication has P > S and causes no such contradiction. After installation, the sender follows current permissions and reading starts at S. An inapplicable delta permits one snapshot recovery attempt; a repeated failure must be visible to the application. An unknown codec requires an update; a storage error requires storage repair.

### Stream lifecycle and epochs

The host creates streams and returns `{stream, epoch}` through its bootstrap/API. For catalogs, bootstrap lists available catalogs and is loaded again after reinstallation. An offline project may remain local. If it needs a separate stream, the server's `createProject` creates one before the first sync operation is prepared. No snapshot is required for a nonexistent stream. The application may create related records offline within an already known stream.

On `stream_not_found`, refresh bootstrap and keep local work paused until the creation outcome is known. On `stream_gone`, stop syncing that address, retain local work, and remove the stream from the active list. Never reuse its address. Check permissions before disclosing information about a closed stream.

After an epoch change, automatically reload a catalog that has no local edits. For editable data, first persist a recovery copy tagged with the old epoch, then obtain a new snapshot to reopen reading. Never send old operations automatically into the new epoch. Application recovery may export the work or create a **new project with a new identity**. Automatic merging of old CRDT history into a restored server base is not guaranteed. Successful sync must not delete the archive as a side effect.

Within an epoch, confirmed history, operation outcomes, and receipts must not roll back or be replaced by a different history. A new owner may retain the epoch only if it preserves these guarantees; a matching `head` is insufficient. Recovery that loses confirmed commits, including failover to a lagging replica, requires a new epoch before serving requests. Fence the former owner from both writes and read responses. [PostgreSQL asynchronous replication can lose commits during failover](https://www.postgresql.org/docs/current/warm-standby.html).

## Server transaction boundaries

The first implementation performs sync checks on the primary. The stream row holds `epoch`, `head`, and `gc_floor`, the position of the removed prefix. All writers and garbage collection serialize through its lock; acquire multiple locks in a consistent order. Permission checks, permission changes, and epoch changes must share this boundary.

Every network exchange and server transaction has a finite deadline. The **data/database owner** enforces the transaction deadline, including lock waits, the application callback, and RPC. A fenced expired handler must not resume and commit later. Roll back its open transaction and release locks within a bounded time. A completed or already started commit may win a race with the timeout; resolve the unknown outcome through the original `op` and receipt. Returning an HTTP error while leaving a stuck transaction alive is insufficient. These rules apply to all writers, garbage collection, and snapshot publication.

### Push and cancel

In one transaction at the data owner:

1. Lock the stream row through commit; check epoch, subject, and read permission.
2. Find `(stream, epoch, op)` in receipts. Check author and hash as specified in [WIRE](./WIRE.md). Return the saved result without calling application code; a mismatch is `operation_changed`.
3. For an unknown `op`, check resource admission. For `cancel`, save `rejected/cancelled` and commit; write permission is unnecessary. For a message, check write permission and format support.
4. Apply the application change inside a savepoint. Verify that the published event is fully applicable to the previous state and that the result can be represented by a valid snapshot. Proven size bounds may establish this without exporting the entire document on every edit.
5. On application rejection, roll back the savepoint, including data and external jobs, then save only the receipt. An unexpected exception rolls back the entire transaction.
6. On acceptance, increment the transactional `head` and save one event, the receipt, and jobs. Respond after commit. Store the original author and request hash with the event so `op/hash` can be exposed to that author. A `noop` event has an explicit reason and no message.

If cancellation wins, a delayed push finds the rejection. If push wins, cancellation returns acceptance. After a crash following commit, a retry reads the saved receipt. Validate outer structure and size before this transaction. Resource, network, and format errors must not become durable application rejections.

Keep the lock until commit so a later position cannot become visible before an earlier one. `nextval()` alone does not guarantee this: allocating a number is not a commit, and rollback does not reclaim it. [PostgreSQL sequence rules](https://www.postgresql.org/docs/current/functions-sequence.html).

RPC to a remote storage owner passes the entire operation. Committing a receipt in the protocol server separately after changing application data at the owner is forbidden. Other APIs and background jobs use the same boundary and resource limits, publishing events with `op: null`. **One atomic application change within a stream is one log entry**, which may contain several record changes. Multiple entries in one database transaction can land on separate pull pages and do not provide client atomicity.

External jobs commit with the data, but a worker may repeat an external action after a crash. The recipient must support idempotency or a deduplication key. [Transactional outbox limitation](https://microservices.io/patterns/data/transactional-outbox.html).

### Pull and garbage collection

A simple implementation uses a short transaction with the same stream lock to check access and epoch, read `gc_floor` and `head`, select whole events, and calculate `more` against the fixed `to`. Materialize the response body before releasing the lock. Garbage collection must not remove part of the selected range between the boundary check and event reads. Two independent SELECTs at READ COMMITTED are insufficient.

Garbage collection removes only a prefix guaranteed to be at least 30 days old **after commit**. Client time and transaction start time are unsuitable. A conservative implementation marks already visible committed events during the first maintenance pass, then counts 30 days from that mark. It never removes unmarked events. Update `gc_floor` atomically with deletion; retain receipts. A valid cursor C satisfies `gc_floor ≤ C ≤ head`.

Events after cut S committed after that cut. While a snapshot is at most 24 hours old, these events remain within the required 30 days of retention. Active snapshots therefore need no separate reservation for future log entries. Losing network access for several days also need not force a new snapshot. A much longer offline period may require one, but must not delete local work.

### Building snapshots without long writer locks

1. Under a short coordination lock, check the active attempt and its parameters **before checking a new snapshot's quota**. Reserve capacity and one build job for a new attempt. Concurrent retries wait for it or receive `snapshot_busy`.
2. In one consistent `REPEATABLE READ` transaction, read the epoch, S = head, and all synchronized data and metadata. Copy them to bounded immutable staging storage without holding `FOR UPDATE` for the copy's duration. Do not publish unfinished pages.
3. In a short final transaction, lock the stream and attempt, recheck current epoch and access, and read receipts for every `pending` operation. Commit snapshot metadata and complete `ops`, then return the page.

Receipt reads may happen after the cut. Acceptance at P ≤ S is in the image; acceptance at P > S will arrive through the log. No receipt at publication proves that the effect is absent from S; a later delayed push receives P > S. This resolves the combination of an unknown operation, revoked write permission, and an expired cursor without inventing a final `not_found` outcome.

Data and S must come from one MVCC state. Two successive READ COMMITTED queries do not provide that guarantee. [PostgreSQL isolation](https://www.postgresql.org/docs/current/transaction-iso.html). Other databases must provide an equivalent consistent cut.

Fix page boundaries with room for metadata. Lifetime is at most 24 hours from capturing S, not from finishing the copy. Lost staging data or snapshots produce `snapshot_expired`, never partial success. Expired or lost results release their slot before a new snapshot's quota check. A build after expiry always has a new server ID. After crashes, jobs must recover or release their reservations within a bounded time.

Builds of the same state may be shared. Reuse a finished image as a new cut only if its epoch and head still match the authoritative state; otherwise, take a new consistent cut. Never present an old copy as fresh merely to extend log retention. Check personalized `ops` when publishing each attempt.

A lagging read replica must not label a valid cursor or epoch invalid. Use the authoritative node, wait, or return `not_caught_up`. [PostgreSQL Hot Standby](https://www.postgresql.org/docs/current/hot-standby.html).

## Resource profile

The initial resource profile targets small record and catalog streams. These are **profile limits, not measured capacity results**. Before deployment, load test your data on the least capable supported devices. Larger records and CRDT documents require a separately validated application profile.

| Resource | Initial catalog profile |
| --- | --- |
| Whole snapshot | Up to 64 MiB of decoded data and 2048 items |
| One event or item | Up to 512 KiB of decoded data; transfer files separately |
| Snapshot creation | 10-second build budget; exceeding it returns `snapshot_busy`, with no partial image |
| Transaction / client exchange deadline | 30 seconds at the data owner / 60 seconds for the entire HTTP exchange, including body reads; a timeout is not operation rejection |
| Concurrency | Up to 2 simultaneous builds per deployment; up to 4 independent streams in the SDK |
| Active snapshots | A limit of one per `(subject, client, stream)` is allowed; retrying the current attempt consumes no second slot |
| Retention | Log: at least 30 days after commit. Snapshot: 600 seconds idle, at most 24 hours total |

Account and server quotas cover all devices and must accommodate the product's supported number of simultaneous devices. One snapshot per account is unsuitable for multiple devices. A random `client` cannot bypass account, rate, or byte limits. New snapshots must not evict promised active ones. Reaching a quota may return 429; the protocol does not guarantee availability under permanent resource exhaustion.

Before launch, set maximum incoming byte and operation rates per stream, account, and server, plus disk capacity and headroom. Include **30 days of accepted log entries, images and staging, receipts, indexes, replicas, and reserve capacity**. Admission checks cover every write path. On insufficient space, temporarily refuse new changes before execution with `storage_pressure`; preserve reads and existing receipt lookup as far as functioning storage permits. Never shorten retention. Persistent shortages require more capacity or less load.

Receipts intentionally last for the entire epoch. At one new operation per second, that is 31,536,000 records per year: about 7.52 GiB at an **illustrative** 256 bytes per record, excluding extra indexes, replicas, and backups. Measure actual storage. Rejections and cancellations also consume space. Rate limits reduce growth but do not bound lifetime storage. A cumulative lifetime user quota does not recover capacity by itself; capacity forecasts, monitoring, and an expansion plan are required. Server catalog updates with `op: null` create no client receipts.

Creating a receipt through cancellation uses the same stable-subject and server budgets even when the subject has only read permission. Returning an existing receipt consumes no new-receipt quota. `storage_pressure` may temporarily prevent a new cancellation: the client retains the unknown operation while confirmed reads stay independent. Cancellation is not guaranteed while storage remains unable to accept writes.

The profile must also define a measured memory limit. JSON, Base64, decoded data, application state, and possibly an import copy coexist. A request body limit is not a RAM limit. Do not silently retain several copies of the full database on a mobile client.

A fixed `to` makes a traversal finite despite new writes. It does not guarantee real-time freshness for a client that consistently reads more slowly than data arrives. Completion requires enough throughput to read the range before its unread beginning is collected and to download a snapshot within its lifetime. Detect violations through load tests and lag metrics instead of hiding them behind repeated snapshots.

Automatic recovery completion requires a running client, restored read access, storage, codec support and service availability, finitely many old requests from abandoned attempts, and exchanges fast enough to obtain a snapshot before expiry. Temporary slot shortages end by expiry. These conditions do not cover an endless influx of competing requests or permanent resource shortages.

The profile defines supported application versions and formats. Publish a new format only when every still-supported reader understands it. Other readers require an explicit update and must not skip the event. A new client retains decoders for every format in the log and snapshots available to it. A transport update does not replace this policy. An unavailable old push handler must not prevent returning an existing receipt.

## Application formats and CRDTs

Replicaman treats payload bytes as application data. A CRDT integration needs
its own profile for document identity, dependencies, snapshot assembly, edit
sizes, retained history, and recovery. The transport's 32 MiB message limit
alone does not establish that such a profile works.

- Validate an incoming delta on an isolated candidate. Missing dependencies
  must not become a partially applied event; reject the change without
  modifying the confirmed base.
- If local operations cannot be removed after merging, publish the exact
  accepted delta bytes. Server normalization or a noop cannot silently replace
  those operations.
- Bound full exports and atomic edits before preparation and acceptance.
  Visible document size may be much smaller than retained CRDT history.
- Define explicit assembly rules for snapshot parts. Splitting a snapshot
  does not make arbitrary pieces of an outgoing delta independently applicable.
- Treat history trimming as a separate recovery problem. Log retention does
  not remove history embedded in application documents.
- Keep an accessible copy of old data and queued work after an incompatible
  history or epoch change. Never silently import old operations into a new base.
- Scope history access deliberately. Sharing current content without deleted
  history requires a separate, sanitized document identity and access scope.

## Model checking

[`formal/check.sh`](../formal/check.sh) runs TLC over finite state graphs with retry and crash cycles, safety invariants, and conditional recovery completion. Joint also combines delivery, cancellation, snapshots, and context changes in one state. The [formal model guide](../formal/README.md) defines the formulas, bounds, assumptions, and implementation obligations. Deliberate defects must produce counterexamples for duplicate execution, skipped events, a lost barrier, permanent pause, stale `unseen` replacing a fresh receipt, and a response from a replaced session changing current state.

These bounded models do not prove the real database or SDK correct, nor establish composition of the whole protocol for arbitrary numbers of events. The implementation checks below remain required.

## Required release scenarios

Conformance tests must control time, garbage collection, transaction order, and crash points. Use this checklist for each host integration; library tests do not certify a deployment.

| Scenario | Required result |
| --- | --- |
| Concurrent identical pushes, refreshed token, changed bytes or author | One execution; original receipt or `operation_changed` |
| Accepted push loses its response; push then fails while pull works | Own event's `op/hash` confirms the operation; reads require no extra push |
| Another author's matching op; own op with another hash; another author's noop | Hide foreign correlation; a wrong hash does not remove the local edit; preserve event order |
| Unknown op without a receipt, revoked write permission, expired cursor | Install a snapshot, continue reading, and retain local work |
| Cut S, acceptance before or after S, delayed push after `unseen` | Count the effect exactly once; never turn `unseen` into rejection |
| Push races cancellation; either response is lost | One immutable outcome; cancelled operations never execute later |
| Deterministic 5xx for one op | Visible failure and automatic retries with backoff; independent reads; cancellation available with functioning storage |
| Callback or RPC hangs while holding a lock; deadline races commit | Fence expired work and release the lock; the receipt determines the outcome, not the HTTP timeout |
| Server normalizes A; intent B depends on A | Prepare B after applying the canonical result; do not skip other authors' events |
| Explicit noop versus an ordinary message with empty bytes | Deliver the noop reason; do not misclassify an empty message as noop |
| Crash while applying a page or snapshot; new edit during preparation | Keep base, queue, archive, and cursor consistent; retain the new edit |
| New op during snapshot download | Defer preparation, retain its intent, and cover the old op correctly |
| Crash after `beginSnapshot`; preparation computed earlier; abandoned and restarted assembly | Preserve the barrier and check it at commit; never resume the old attempt with a new queue |
| First snapshot response lost with a one-slot quota | Retry returns the same active snapshot without consuming another slot |
| Attempt A abandoned under a one-slot quota; B gets 429; eight failures, then service recovers | A expires; B continues automatically according to `Retry-After`; old responses are never installed |
| Changed `limit/pending`; expired attempt; old page arrives | Reject changed parameters; never mix different snapshots |
| Garbage collection races pull; final snapshot page is lost | Never return a partial page; retain the log tail needed after a valid snapshot |
| Writes outpace reads, numeric gaps, 24-hour expiry | Keep `to` fixed; distinguish traversal completion from freshness limits |
| Copying a snapshot during concurrent commits | Keep S and data consistent, with published events in order |
| Snapshot S plus log through T versus an independent snapshot T | Equivalent data and metadata for subsequent application |
| Account or epoch change, old response, lagging replica | No writes into a new context; lag does not become a permanent error |
| Failover loses a confirmed commit or replaces history at the same head | Change epoch before serving requests; fence the former owner from reads and writes |
| Stream creation or deletion, app reinstallation | Bootstrap restores the list; retain local work |
| Multiple devices, simultaneous startup, disk pressure from other APIs | Never evict promised snapshots; enforce shared limits; preserve log retention |
| Application rejection; external job retried after a crash | Rejection has no effects; the external recipient deduplicates retries |
| Page, growing snapshot, RAM, and outgoing edit limits | Verify on real devices; exceeding limits must not cause endless sending |
| Different outer JSON; changed type or payload; noncanonical Base64 | Same hash for equivalent outer JSON, different hash for changed content; reject ambiguous encoding |
| Remaining TTL 0.75 seconds; `to` above authoritative head; null in CloudEvents | `snapshot_expired`; `cursor_invalid`; rejection before conversion and hashing, respectively |
| Complete malformed 200 response versus network interruption and late response | Distinguish pause, automatic retry with backoff, and ignoring the response |
| CRDT dependencies; old edits after trimming or epoch change | No false application; an accessible recovery copy remains |
| New participant after data was deleted | History access matches the declared permission scope |

The Rust protocol core is implemented and tested against an in-memory owner. A host integration must verify its durable transactions, application format, account isolation, and deployment capacity separately. The protocol and finite models support that work; they do not certify an arbitrary storage adapter or establish a public compatibility guarantee.
