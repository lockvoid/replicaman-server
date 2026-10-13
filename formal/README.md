# Replicaman formal models

These [TLA+ models and TLC checks](https://lamport.azurewebsites.net/tla/tools.html) cover bounded parts of the [protocol](../docs/PROTOCOL.md), [wire format](../docs/WIRE.md), and [implementation requirements](../docs/IMPLEMENTATION.md). TLC explores reachable states and checks cycles where required. The models allow repeated actions and infinite executions; they do not just enumerate permutations of one-shot actions.

```sh
./formal/check.sh
```

Run from the repository root. Requirements: Java (tested with 25), Bash, curl, and shasum. The [runner](./check.sh) downloads official `tla2tools` 1.7.4, verifies its pinned SHA-256, and runs four positive and six negative checks. `TLA_JAR` selects an existing jar, which must pass the same checksum check; `JAVA_BIN` selects Java. Logs and counterexample traces are stored outside the source tree. Negative checks require the expected exit status and violation text; a syntax error or tool crash does not count as a counterexample.

## Specification and properties

```text
Spec ≜ Init ∧ □[Next]vars ∧ Fair
Next ≜ Action₁ ∨ Action₂ ∨ … ∨ Actionₙ
[Next]vars ≜ Next ∨ UNCHANGED vars
```

`□P` means always P; `◇P` means eventually P; `P ⇝ Q` means P leads to Q. `Fair` states progress assumptions. The safety models have no fairness requirement: losing replies forever must not corrupt data. Recovery needs explicit assumptions to guarantee completion.

| Requirement | Formula or model property |
| --- | --- |
| An op is accepted at most once per epoch | `∀op: commits[op] ≤ 1` — `Delivery.AtMostOnce` |
| Rejection has no effect; receipts are immutable | `RejectedNoEffect`, `ReceiptStable`, `AtomicOutcome` |
| Confirmed data equals the consumed log prefix | `base = Prefix(cursor)` — `Read.ConfirmedPrefix` |
| Local work is represented exactly once | `copies(base, own) + activeOverlay = savedIntent` — `LocalConservation` |
| A snapshot matches S and accounts for prepared operations | `SnapshotSound`, `SnapshotCoverage`, `FixedPending`, `DurableBarrier` |
| Wrong epoch/identity and stale responses cannot alter the current context | `OldEpochNoEffect`, `ChangedIdentityNoEffect`, `ContextIsolated` |
| Cancel, snapshots, and replies preserve work and known outcomes | `Joint.WorkConserved`, `KnownReceiptNeverRegresses`, `OldResponseNoMutation` |
| Recovery completes under the availability assumptions | `◇(phase = "done")` — `Recovery.EventualCompletion` |

`Prefix` is an **ordered sequence** of distinguishable events, not a numeric sum. Missing, reordered, and repeated events remain observable. For real data, the application must satisfy `Apply(Snapshot(S), Log(S,T]) ≡ Snapshot(T)`, including metadata. The models assume this law and atomic transactional callbacks.

## Finite bounds

| Model and configuration | State and transitions | Configured bounds |
| --- | --- | --- |
| [Delivery](./Delivery.tla) · [config](./Delivery.cfg) | Receipts, separate execution/effect counters, permissions, queued replies, context; push/cancel, retry, loss/delivery, permission and epoch changes | One stream; two ops, subjects, and hashes; two epochs that never repeat; at most two distinct queued replies; counters saturate at 2 |
| [Read](./Read.tla) · [config](./Read.cfg) | Log, age/GC, base/cursor/local work, barrier, snapshot, pull; preparation, commit, pages, cut/publish/install, crash/restart/retry | One stream/epoch/client; two foreign events, one own event, one numeric position gap; retention 2 ticks, snapshot lifetime 1 tick |
| [Recovery](./Recovery.tla) · [config](./Recovery.cfg) | Phase, one snapshot slot, TTL/lifetime/pages/delays/failures; lost creation response, quota wait, expiry, retry, service recovery | Two pages; one delayed request from an abandoned attempt; idle TTL 4 ticks, hard lifetime 12, processing delay ≤1 tick, backoff 2, notification threshold 8 |
| [Joint](./Joint.tla) · [config](./Joint.cfg) | Shared log, receipt, work/archive, context, reply buffer, snapshot; push/cancel, cut/publish/install, delivery, context change, crash | One op bound to the first epoch, one foreign event total; two epochs and two sessions, one client context change; one reply and one single-page snapshot; one snapshot attempt per session |

Retry and restart cycles have no execution-length limit. The graphs are finite because their variables have bounded reachable domains, without a `STATE CONSTRAINT` that truncates exploration. Only Delivery uses symmetry over interchangeable labels; Recovery's temporal checks do not. Saturation at 2 keeps a second execution detectable.

Recovery starts with an abandoned snapshot occupying the only slot and service unavailable. `WF(Heal)` assumes service eventually recovers and stays available. Time advances, the single stale request is eventually processed, and enabled work receives service. The `age ≤ Delay` bound limits processing delay; `Lease > Delay + 1` and `Lifetime > (Pages + 2) × (Delay + 1)` allow time to finish. **These are environmental assumptions, not evidence of network performance.** Human `resume` is not assumed fair.

After the last stale request, the old slot can no longer be renewed and expires. Automatic retries continue, and the new attempt can fetch and install its pages within the stated bounds. Fair scheduling alone, without a delay bound, would not guarantee completion before expiry.

Joint checks interference in **one shared state**: late push/cancel, immutable `snapshot.ops` alongside a newer client receipt, and old-session replies after context replacement. Archived bytes may coexist with a canonical effect; the archive is not a second visible effect. Its single client context change is either a replacement recovery session or a move to epoch 2 with archival, not both in sequence. An old snapshot slot must be discarded before the next attempt starts. Read and Recovery check timing, GC, and liveness separately; no formal composition with Joint is claimed.

## Expected counterexamples

| Deliberate defect | Required TLC result |
| --- | --- |
| [DeliveryBrokenDedup](./DeliveryBrokenDedup.cfg) | Two identical pushes produce `commits[op] = 2`, violating `AtMostOnce` |
| [ReadBrokenCursor](./ReadBrokenCursor.cfg) | Advancing the cursor while skipping an event violates `ConfirmedPrefix` |
| [ReadBrokenBarrier](./ReadBrokenBarrier.cfg) | Begin → crash loses the barrier → new op → acceptance → snapshot install leaves the own edit in both base and overlay, violating `LocalConservation` |
| [JointBrokenUnseen](./JointBrokenUnseen.cfg) | Published `unseen` → acceptance → fresh receipt → install overwrites that receipt with `unseen`, violating `KnownReceiptNeverRegresses` |
| [JointBrokenSession](./JointBrokenSession.cfg) | A pull reply for session 1 changes client state after replacement with session 2, violating `OldResponseNoMutation` |
| [RecoveryLegacy](./RecoveryLegacy.cfg) | Eight failures → paused → service recovers → slot expires → indefinite wait without resume, violating `EventualCompletion` |

The current protocol reports the eighth consecutive transient failure but keeps retrying automatically. The legacy variant preserves the permanent-pause defect as a regression check.

## Proof boundary

A defect report should identify the violated property, transitions, and allowed environmental conditions. Model defects need a reproducible configuration and trace; implementation defects need a component test.

| Concern | Coverage or obligation |
| --- | --- |
| Push/cancel retries, write revocation, author/hash mismatch | Delivery; Read also checks acceptance before/after S and after `unseen` |
| Cursor, GC, snapshot, local work, crash | Read; `LiveSnapshotTailRetained` relates snapshot age to log retention |
| Cancel/new receipts during assembly; replies from a replaced context | Joint's shared state, work/archive, known outcomes, and stale-response observer |
| Abandoned slot, lost creation response, repeated transient failures | Recovery, with finite stale requests and stable service after Heal |
| Lost commits during failover; atomic background writes | Implementation obligations: fresh epoch after history loss, fencing the old owner, one event per atomic update |
| TTL below one second, `to > head`, null/hash/JSON, format versions | Wire contract and codec/HTTP tests; these models do not verify parsing or real-time durations |
| Receipt/cancel quotas, storage capacity, download throughput | Admission and availability conditions in the implementation requirements; permanent failure does not satisfy recovery assumptions |
| Application conflicts, CRDTs, files, size limits | Application law and separately validated resource profiles |

TLC checks all reachable states and required cycles **of these configurations**, not every size of the real protocol. Its use of state fingerprints retains a technical collision risk. The negative checks show sensitivity to six defects, not completeness of the property set.

No cutoff theorem, general composition theorem, or refinement from an implementation to these models has been proved. Joint checks the specified interactions within its bounds; its result does not automatically extend to arbitrary operations, context changes, or timers. A proof for all sizes needs inductive arguments and implementation refinement. Release also requires tests of real transactions, codecs, and the application law.

## Readiness

The transport design criterion is met: known counterexamples have explicit protocol rules, all four positive configurations pass, all six deliberate defects produce the expected violations, and the bounds and assumptions are documented. The **transport contract is ready to implement**; this does not claim a proof for all sizes.

The Rust server implementation and its tests are in this repository. Host and SDK release readiness also depends on the conformance checks in the [implementation requirements](../docs/IMPLEMENTATION.md). These models do not verify a database adapter, application codec, or CRDT integration.
