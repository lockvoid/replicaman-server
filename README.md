# Replicaman server

Replicaman synchronizes application data through ordered events and snapshots.
A client can retry a change after losing its response without applying that
change twice. If its cursor is too old, it downloads a snapshot and continues
from the snapshot's position.

This repository contains the v1 protocol, an embeddable Rust/Axum server,
conformance tests, and TLA+ models. The library handles the wire format and
protocol decisions. Your application owns authentication, access rules,
payload semantics, and durable storage.

## Try the example

Requires Rust 1.94 or later. From the repository root:

```sh
REPLICAMAN_DEMO_TOKEN=local-development cargo run --example server
```

The example listens on `127.0.0.1:4100` and prints its stream and epoch. It keeps
all data in memory; restarting loses that data and creates a new epoch. The
[server guide](docs/SERVER.md#run-locally) includes a first snapshot request and
the example's application payloads.

## How synchronization works

| Endpoint | Purpose |
| --- | --- |
| `POST /replicaman/v1/push` | Propose a change or cancel an operation whose outcome is unknown. Retries use the same operation ID and bytes. |
| `POST /replicaman/v1/pull` | Read a complete, ordered prefix of committed events. Save application data and cursor together. |
| `POST /replicaman/v1/snapshot` | Obtain a complete base and the outcomes of pending operations. Install all pages in one application transaction. |

A stream contains data with the same access rights and event order. There is no
ordering across streams. The application defines what a message means; the
server does not generate models or require a particular database or ORM.

## Embed the server

Add a local dependency to your application's `Cargo.toml`:

```toml
[dependencies]
replicaman-server = { path = "../replicaman-server" }
```

Supply a `backend::Backend` and an `http::Authenticator`, then mount
`http::router(backend, authenticator)` in your Axum application. The
[storage integration guide](docs/SERVER.md#embed-and-connect-storage) describes
the interfaces and deadlines.

The storage owner must commit the application change, event, head, and receipt
in the same transaction. A successful HTTP response alone cannot provide that
guarantee. The example backend illustrates the contract but is not a durable
storage implementation.

## Documentation

| Document | Use it to |
| --- | --- |
| [Protocol](docs/PROTOCOL.md) | Understand delivery, ordering, cancellation, and recovery. |
| [Wire format](docs/WIRE.md) | Implement exact fields, hashing, limits, and error handling. |
| [Rust server guide](docs/SERVER.md) | Run the example and connect a storage owner. |
| [Implementation requirements](docs/IMPLEMENTATION.md) | Design transactions, choose resource limits, and validate a deployment. |
| [Formal models](formal/README.md) | Run bounded model checks and understand their assumptions. |
| [Wire fixtures](docs/fixtures/wire.json) | Check portable hashing and encoding behavior. |

## Develop and test

```sh
cargo fmt --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
./formal/check.sh
```

The model runner requires Java, Bash, curl, and shasum. It downloads a pinned
TLC jar and verifies its checksum; see the [model guide](formal/README.md).

This is a pre-1.0 library. Server tests use an in-memory owner, and the formal
models explore finite configurations. Validate your database transactions,
crash recovery, capacity, and application payload rules before deploying a
production host.

## License

[MIT](LICENSE).
