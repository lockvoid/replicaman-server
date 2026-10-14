# Rust server

The crate implements the wire codec, HTTP endpoints, delivery decisions, and immutable snapshot pages. It has no database, ORM, generated models, or application document dependency. The application owns data, access rules, transactions, and the meaning of each message.

## Run locally

```sh
export REPLICAMAN_DEMO_TOKEN=local-development
cargo run --example server
```

The example binds `127.0.0.1:4100`, prints bootstrap metadata for `demo/kv`, and authenticates the token as the stable subject `demo-user`. **All example data is process-local.** Restarting loses it and generates a new epoch; the example is not a persistence backend.

`REPLICAMAN_DEMO_BIND` overrides the listen address. The example stops on Ctrl-C
or SIGTERM. A process supervisor may set `REPLICAMAN_SUPERVISED=1` and supply an
open stdin pipe: EOF also requests shutdown. Standalone execution does not treat
an empty stdin as a shutdown request.

Copy the printed epoch into this request:

```sh
curl http://127.0.0.1:4100/replicaman/v1/snapshot \
  -H "Authorization: Bearer $REPLICAMAN_DEMO_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"stream":"demo/kv","epoch":"<printed epoch>","client":"4234dfb9-6247-46bc-9a65-376c82e87b3e","attempt":"c0d99b3e-02be-4d94-a575-c54670e267aa","page":null}'
```

An empty new stream returns an empty snapshot at position `"0"`. Submit changes using the [push envelope](./WIRE.md#push), with CloudEvents `type: "replicaman.demo.kv.v1"` and `datacontenttype: "application/json"`. Base64-encode one of these application payloads:

```json
{"action":"set","key":"tea","value":" oolong "}
{"action":"delete","key":"tea"}
```

The example trims values and publishes the canonical change. Setting the existing value or deleting a missing key produces an explicit noop. Snapshot items are set operations applied to an empty map. These rules belong to the example, not to the protocol.

The example deliberately copies a small candidate state under one mutex. Errors, panics, and expired owner deadlines discard the candidate. Recorded external jobs are never dispatched. Production snapshot staging follows [IMPLEMENTATION.md](./IMPLEMENTATION.md#building-snapshots-without-long-writer-locks).

## Embed and connect storage

Use `http::router(backend, authenticator)` in an Axum application. Serve it behind HTTPS. `Authenticator` returns an `AuthIdentity` with a stable subject and an optional opaque `AuthProof`. The proof is scoped to the exact request and redacted from debug output; the data owner can use it to recheck current access in its transaction. A refreshed token must identify the same subject. `http::router_with_config` exposes exchange deadlines, owner deadlines, and request concurrency. Excess concurrent requests receive 429 `rate_limited` with `Retry-After`. Compressed request bodies are rejected with 415; the implementation accepts identity encoding, as required by the [wire format](./WIRE.md#size-limits).

| Interface | Host responsibility |
| --- | --- |
| `backend::Backend` | Execute each whole command at the authoritative data owner. A remote implementation may forward the command over RPC. |
| `engine::ReadTransaction` | Check access and epoch and read metadata, events, and receipts from one consistent stream state. Return a complete ordered event prefix. |
| `engine::WriteTransaction` | Serialize commits; include domain data, jobs, event, head, and receipt in one transaction. Roll back on error or panic. |
| `snapshot::Snapshot` | Store bounded immutable pages; look up an active attempt before admission, coalesce creation, and preserve promised expiry. |
| `snapshot::SnapshotPager` | Pack a cut into pages while reading it, so each page is stored as soon as it is complete. |

For a Rust data owner, call `engine::push` or `engine::pull` inside these transactions. Commit only a successful result, after enforcing `Context::deadline` at the owner. `begin_application` and `rollback_application` form a savepoint over both domain data and external jobs: an application rejection saves only its receipt, while a noop publishes one event without changing the base.

Known receipts are returned before new-operation admission, write permission, or the application handler. Quotas include new cancellations and rejections. Keep receipts throughout the epoch; collect only a committed log prefix at least 30 days old. Resource checks also cover server-side writers, which publish one event per atomic change without a client receipt.

`ApplicationOutcome::Rejected(reason)` is a durable domain decision. Use it for an invalid edit or an application size limit. Temporary storage shortages and unsupported formats return errors without receipts. `validate_state` is a final application check that the candidate still has a representable snapshot; it does not replace domain validation.

For a remote data owner, the RPC boundary carries the **entire** operation. That owner commits data, log, head, and receipt together and returns the result. A separate receipt commit in the HTTP server would break the atomicity guarantee.

HTTP defaults are 60 seconds per exchange and 30 seconds per owner transaction. Parsing and serialization are bounded by body limits and request admission. Timing out the HTTP future does not establish rollback or cancel a remote database transaction. The owner must fence expired work, release locks, and leave the receipt authoritative if a commit raced the timeout.

### Snapshot publication

Exceeding the 10-second snapshot build budget returns 503 `snapshot_busy` with `Retry-After`, without publishing a partial image. An earlier owner or HTTP exchange timeout remains 500 `internal_error`.

1. Check read access and the authoritative epoch. Call `TokenKey::verify` before looking up a continuation; this distinguishes a foreign token from a valid token whose staged image is gone.
2. Return the current active attempt before checking quotas. For a new attempt, reserve bounded capacity and capture `SnapshotCut` from one consistent data/head state.
3. Recheck authority and read `engine::coverage` in the publication transaction. The cut may precede these receipt reads; an accepted position above the cut arrives through the log.
4. Build `Snapshot::new`, persist it, and use `Snapshot::page` for both creation retries and continuation. Commit renewal only after successful checks. Expired/lost images release capacity; an abandoned but active image retains its slot until expiry.

An owner may instead build one image per `(stream, epoch, head, limit)` and publish it for many attempts. Pack it with `SnapshotPager` using `envelope_reserve(stream)`, store the pages without their envelope, and assemble each response from the attempt: its own `snapshot` ID, its `ops` from receipts read at its publication, and `TokenKey::page_token` for `next`. Reuse a finished image only while its epoch and head equal the stream's current state and enough of its 24-hour lifetime remains; check quotas for every new attempt, and let one builder per key hold the build so concurrent requests wait instead of duplicating it.

Error responses carry their `ErrorCode` as a response extension, so host telemetry can tell expected backpressure such as `snapshot_busy` from failures.

Use a trusted nondecreasing owner clock for millisecond arguments. Persist `TokenKey` securely for the lifetime of the served epochs; all serving nodes need the same key. Its signed opaque tokens bind subject, stream, epoch, installation, attempt, snapshot, and page. No permanent snapshot receipt is required.

`SnapshotLimits::default()` applies the initial catalog bounds: 2048 items, 64 MiB total decoded data, and 512 KiB per item. The host enforces those limits on every writer, plus the 10-second build budget, deployment/account quotas, and the complete [resource profile](./IMPLEMENTATION.md#resource-profile). The library's paging helper enforces the wire body limits; it does not independently guarantee production capacity.

## Verification scope

```sh
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

Tests exercise strict wire decoding and the shared hash fixture; HTTP authentication, errors, limits and deadlines; transactional deduplication, cancellation and rollback; ordered pages; snapshot replay, operation coverage, token binding, TTL, and slot release. The memory owner supports controlled time and injected failures so these cases have reproducible outcomes.

The [TLA+ models](../formal/README.md) check bounded protocol states separately. The models and memory-backend tests do not establish database crash durability, failover fencing, RPC behavior, real-device performance, or the correctness of a mobile storage adapter. Validate those properties in your host using the [release scenarios](./IMPLEMENTATION.md#required-release-scenarios). Application formats, including CRDT formats, require their own integration and resource profile.
