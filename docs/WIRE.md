# Replicaman v1 wire format

Normative companion to [PROTOCOL.md](./PROTOCOL.md). A `?` marks an optional field; all other fields are required. Unlisted combinations are forbidden. Reject unknown request fields. Ignore unknown response fields, but require a client update for unknown `status`, `result`, or event variants. CloudEvents extension attributes follow the rules below.

## Transport and types

Use HTTPS, `POST /replicaman/v1/{push,pull,snapshot}`, UTF-8 JSON, `Content-Type: application/json`, and `Cache-Control: no-store`. The application supplies authorization. Neither `source` nor `client` proves identity. Every request includes the concrete epoch obtained with its stream from application bootstrap. All methods check read access and the current epoch; a new change also requires write access.

| Field or type | Constraint |
| --- | --- |
| `stream` | Opaque ASCII identifier matching `[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}`. |
| `epoch`, `op`, `client`, `attempt`, `snapshot`, CloudEvents `id` | Canonical lowercase UUID v4; exact comparison. |
| Position: `cursor`, `to`, `head` | Decimal **string** from `"0"` to `"9223372036854775807"`, with no leading zeros; numeric comparison. |
| `hash` | SHA-256 encoded as 64 lowercase hexadecimal characters. |
| Code: `reason`, `error` | `[a-z][a-z0-9_]{0,63}`; the application supplies localized text. |
| `limit` | Integer 1–100, default 100; an upper bound on item count, not a promised page size. |
| Page token | Opaque ASCII string, 1–2048 bytes; clients do not parse it. |

Never reuse an epoch, including after backup restoration. Create a `client` ID per installation's local store; devices do not share it. Never reuse an operation ID for a new intent. Published `head` cannot decrease within an epoch. Pause writes on position exhaustion; do not wrap the counter.

Reject invalid UTF-8, lone surrogates, duplicate keys in any JSON object, and non-finite numbers. Detect duplicate keys **after unescaping and before constructing a dictionary**; ordinary model decoding is insufficient. These rules apply to the outer JSON. The application validates payload bytes.

## CloudEvents message

Use [CloudEvents 1.0.2](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md) with `specversion: "1.0"`. This is a JSON profile embedded in Replicaman, not the CloudEvents HTTP binding. The application defines `type` and payload semantics.

| Field | Profile |
| --- | --- |
| `specversion` | Exactly `"1.0"`. |
| `id` | Operation UUID in a request; a new server UUID for a committed event or snapshot item. |
| `source` | Nonempty URI-reference, at most 1024 UTF-8 bytes; an origin identifier, not an authorization subject. |
| `type` | Nonempty string, at most 255 UTF-8 bytes; for example, `example.task.rename.v1`. |
| `datacontenttype` | Nonempty valid media type, at most 255 bytes. |
| `data_base64` | Required string containing application bytes; an empty string is allowed. |
| `subject?`, `time?`, `dataschema?` | Standard CloudEvents attributes with their standard types and constraints. |
| Extension attributes | Names match `[a-z][a-z0-9]{0,19}`. Values are strings, booleans, or signed 32-bit integers; arrays, objects, and null are forbidden. |

The canonicalized header, excluding `data_base64`, is at most 8 KiB. All accepted attributes participate in the hash. **Null is forbidden in every message attribute**, including optional standard attributes; omit absent fields. Validate this before SDK conversion or hashing. This narrows the [CloudEvents JSON format](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/formats/json-format.md), where null can mean absence. The `data` field is forbidden: even application JSON travels as UTF-8 bytes in `data_base64`.

Base64 uses the standard RFC 4648 alphabet, required `=` padding, no whitespace, and zero unused bits. The decoder must verify `encode(decode(value)) == value`: `AA==` is valid; `AB==` is not. See [RFC 4648 §3.5](https://www.rfc-editor.org/rfc/rfc4648.html#section-3.5).

### Message hash

```text
header = message without data_base64
hash = lowercase_hex(SHA256(
  UTF8("replicaman-message/1") || 0x00 ||
  JCS(header) || 0x00 || Base64Decode(message.data_base64)
))
```

`JCS` produces canonical UTF-8 bytes under [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785.html). Outer JSON field order, whitespace, and escaping do not affect the hash. The hash includes `type`, all other attributes, and decoded payload bytes. JSON **inside the payload is not canonicalized**. The receipt key is `(stream, epoch, op)`; store the author separately. The access token is not part of the hash.

The Push example below has hash `05f36349252e770165ebfa85976530f7361e2c8d85e2603b99c489ffb8a17025`. Portable [wire fixtures](./fixtures/wire.json) retain this example, canonical header bytes, Base64 cases, and the size calculation.

## Size limits

`MiB = 2²⁰` bytes. One decoded `data_base64` is at most 32 MiB. Any request or response body, including JSON and Base64, is at most 48 MiB **after HTTP decompression**.

HTTP compression is optional. Servers must accept uncompressed requests (no `Content-Encoding`, or `identity`); clients use another encoding only when server support is known. An unsupported request encoding returns 415 `unsupported_media_type`. The decoded body limit applies whenever compression is supported.

A normal pull or snapshot page is at most 1 MiB for the entire body. If the next item cannot fit in an otherwise empty normal page, return that item **alone**, in a body of at most 48 MiB. Do not skip it or split an event in the transport. Encoding 32 MiB produces 44,739,244 Base64 bytes, fitting this limit with the bounded metadata. End a page before exceeding any limit.

Every reachable application state must have a valid snapshot. Multiple snapshot items may represent parts of one document only if the application defines assembly rules. The [resource profile](./IMPLEMENTATION.md#resource-profile) also bounds total snapshot size, document history, memory, and outbound atomic changes.

## Push

Request: `stream`, `epoch`, and **exactly one** of:

- `message: CloudEvent`: submit an operation; `op = message.id`.
- `cancel: {op, hash}`: reject an as-yet unknown operation; `hash` belongs to its original message.

Example; the `{}` payload only illustrates the envelope and does not define an application schema:

```json
{
  "stream": "projects/example",
  "epoch": "f4c05d55-70c7-41d9-9868-c0582ae3a020",
  "message": {
    "specversion": "1.0",
    "id": "13fb2e27-3bf1-4415-b5a4-904e37b6494c",
    "source": "urn:example:client:example",
    "type": "example.change.v1",
    "datacontenttype": "application/json",
    "data_base64": "e30="
  }
}
```

**Every receipt uses HTTP 200**, including application rejection. Common fields: `stream`, `epoch`, `op`, `status`.

| `status` | Remaining fields |
| --- | --- |
| `accepted` | `cursor: Position`, `result: "applied"`; no `reason`. |
| `accepted` | `cursor: Position`, `result: "noop"`, `reason: Code`. |
| `rejected` | `reason: Code`; no `cursor` or `result`. |

Acceptance creates exactly one event. A `noop` is an explicitly accepted result with no base change, such as an already satisfied request; refusing to perform an action is `rejected`. Cancelling an unknown operation saves `rejected/cancelled` without an event. Cancelling a known operation returns its existing receipt. Both paths verify the author and hash.

Return an existing receipt before checking **write** access, invoking the application handler, or applying **new-operation** quotas. Read access and general overload protection still apply. An unavailable old handler after a server upgrade must not prevent receipt retrieval.

## Pull

Request: `{stream, epoch, cursor, limit?, to?}`. Omit `to` or pass null only when choosing a new target.

HTTP 200 response: `{stream, epoch, events, cursor, to, head, more}`.

| Event kind | Fields |
| --- | --- |
| Change | `{cursor, op: UUID|null, hash: Hash|null, message: CloudEvent}` |
| Accepted without change | `{cursor, op: UUID|null, hash: Hash|null, message: null, reason: Code}` |

`op` and `hash` are either both present with non-null values or both null. Populate them only for operations authored by the **current stable authorization subject**; use null for another author's operation or a write without a client op. The hash identifies the original accepted request, not the server's `message`. All readers receive the same events, positions, messages, and reasons. Only `op/hash` are personalized; events are never filtered.

An `op/hash` match with a prepared operation establishes `accepted` at the event position: `applied` for a message, `noop` for null. Save this receipt while applying the page, without another request. A mismatched hash does not confirm the local operation: preserve its work for recovery and continue reading. The server CloudEvent ID differs from `op`.

A null `message` bypasses the payload decoder but passes `reason` to the application, including for another author's noop. Empty `data_base64` in a non-null message is ordinary application data, not a transport noop.

Preserve a supplied `to`; otherwise capture `to = head` from a consistent read. Every successful response satisfies:

```text
request.cursor ≤ response.cursor ≤ to ≤ head
```

Events are a complete prefix of the events in `(request.cursor, to]`, in strictly increasing position order, with no omissions. Every event position is at most `response.cursor`.

- `more: true`: events remain before or at `to`; the page is nonempty and its cursor equals the last event position.
- `more: false`: the cursor equals `to`, even if the final event is earlier or the interval contains no events.

Clients may change `limit` between pull pages. A cursor below the removed prefix produces `cursor_expired`. Authoritative `cursor > head`, `to < cursor`, or `to > head` produces `cursor_invalid`. Numeric gaps within the valid range are allowed. A lagging serving node cannot establish invalidity.

## Snapshot

Initial request: `{stream, epoch, client, attempt, page: null, limit?, pending?}`. `pending` defaults to `[]`: at most 100 `{op, hash}` pairs from the same epoch, unique by op. Retry the **original request** after a lost response.

Until installation or abandonment, do not prepare new operations; keep saving local intents. Persist the preparation barrier, attempt, and complete pending list atomically with respect to operation preparation, and restore them after a crash. Abandonment invalidates the local recovery session and attempt. The next attempt uses a new ID and the current pending list.

Continuation request: `{stream, epoch, client, attempt, snapshot, page: Token, limit?}`. Omit `pending`. Omit `limit` or repeat its initial value. Tokens are bound to the subject, client, stream, epoch, attempt, and specific snapshot. A foreign or malformed token produces `bad_request`; a valid but lost or expired token produces `snapshot_expired`.

HTTP 200 response: `{stream, epoch, client, attempt, snapshot, cursor, items, next, ttl, ops}`.

| Field | Meaning |
| --- | --- |
| `cursor` | Snapshot position S. |
| `items` | Array of CloudEvents. |
| `next` | Page token or null; a non-null token requires a nonempty page. |
| `ttl` | Integer 1–600: remaining lifetime in seconds, rounded down. |
| `ops` | Exactly one result per requested `{op, hash}`, in request order. Each contains `op`, `hash`, and either receipt fields (`status/cursor/result/reason`), `status: "unseen"`, or `status: "mismatch"` with no other fields. Mismatch does not disclose another author's receipt. |

Successful page delivery renews expiry to `min(now + 600 seconds, snapshot cut time + 24 hours)`. If less than one second remains before or after renewal, treat the snapshot as expired: return `snapshot_expired` without a page. Never renew an expired snapshot. The server determines expiry; the client conservatively measures `ttl` from the request's start using a monotonic clock.

S, content, ordering, page boundaries, `next`, and `ops` are immutable for a snapshot ID; only `ttl` may change. The last page does not immediately release the snapshot. Validate all pages and their context before atomic installation. If the client's conservative deadline has expired, obtain a new snapshot.

Within the current epoch, `(subject, client, stream, attempt)` identifies an active attempt. Repeating creation with the same effective `limit` and ordered `pending` returns the same snapshot before checking the new-snapshot quota. Changed parameters produce `attempt_changed`. Coalesce concurrent retries into one creation. Do not serve an old snapshot after an epoch change.

An abandoned attempt occupies its slot until expiry. If it alone blocks a new one, return `429 rate_limited` with `Retry-After` until the next slot release, rounded up to whole seconds with a minimum of 1. Rejecting a new request does not renew the old attempt. The client stops issuing requests for the abandoned attempt and automatically retries the new one after the delay. Already-sent old requests can delay release, but cannot evict an active new snapshot.

After expiry or loss, retrying `page: null` may create a new snapshot with a new server ID. The attempt ID is **not a permanent content identity**. An intentional new creation uses a new attempt ID. Always separate assemblies by snapshot ID and local session generation. Snapshot creation does not require permanent receipts.

## Errors and response validation

Errors have `{error: Code}`. For `epoch_changed`, `epoch` is also required and identifies the current generation. An error response is **not an operation receipt**: this attempt not executing does not prove that an earlier attempt cannot execute later.

| HTTP | `error` | Client action |
| --- | --- | --- |
| 400 | `bad_request`, `unsupported_protocol` | Fix the request/client; preserve the ID of an operation with unknown outcome. |
| 401 | `unauthorized` | Refresh authorization; change the local session if the subject changes. |
| 403 | `stream_forbidden` | Pause the stream until access is restored. |
| 403 | `write_forbidden` | Pause sending; continue reading and allow cancellation. |
| 404 / 410 | `stream_not_found` / `stream_gone` | Refresh bootstrap / close the stream, preserving local work. |
| 409 | `epoch_changed` | Recover under the epoch-change rules. |
| 409 | `cursor_expired`, `snapshot_expired` | Obtain a new snapshot; preserve local work. |
| 409 | `cursor_invalid` | History is immutable within an epoch, so treat this as damaged local state: preserve local work, attempt one snapshot recovery, then expose a repeated failure. |
| 409 | `attempt_changed`, `operation_changed` | Expose a state error; isolate disputed work instead of retrying indefinitely. |
| 413 | `payload_too_large` | Fix application size limits; resolve or cancel an already prepared operation before replacing it. |
| 415 | `unsupported_media_type` | Fix the outer Content-Type/Content-Encoding. |
| 422 | `unsupported_format` | Install a handler supporting the CloudEvents version, application type, or payload format. |
| 429 | `rate_limited` | Retry automatically with mandatory `Retry-After`. |
| 503 | `not_caught_up`, `storage_pressure`, `snapshot_busy` | Retry automatically with mandatory `Retry-After`. |
| 500 | `internal_error` | Retry automatically with backoff; do not infer rejection. |

Check 400/413/415 conditions before execution. A 422 creates no receipt; an existing receipt is returned before application-format checks. Syntactically valid data that violates **domain rules** produces a durable `rejected`, not repeated 5xx. The application defines its reason codes; `cancelled` is reserved.

HTTP 426 is not used: it requests a connection-protocol switch using `Upgrade`, rather than an application-handler update. See [RFC 9110 §15.5.22](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.5.22).

First discard replies for an old local session or superseded request. For the current response, validate HTTP status, structure, context, limits, positions, `ops`, consistency with saved receipts, and successful application. Contradictory receipts are a protocol error. On failure, leave the confirmed base, queue, and cursor unchanged.

Automatically retry disconnects, incomplete bodies, 408, 429, and all 5xx with backoff, including proxy errors without JSON. Failure count never introduces a manual pause. A complete malformed 200 body, unknown 4xx error code, or unexpected 2xx is a visible protocol error until repaired; this is distinct from a stale reply.
