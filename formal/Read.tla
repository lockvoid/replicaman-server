------------------------------- MODULE Read -------------------------------
EXTENDS Naturals, Integers, Sequences, FiniteSets, TLC

(***************************************************************************
 Bounded SAFETY model of the read and snapshot rules specified in docs/.
 The graph contains request/loss/retry, crash/restart, and abandon/recreate
 cycles; it does not enumerate a list of one-shot action permutations.

 Bounds: one stream/epoch/client, two ordered foreign events, one local op,
 at most one numeric position gap. A local op has an optimistic overlay and
 a distinct canonical event token. Confirmed application state is an ORDERED
 token sequence, so omission, duplication, and reordering are observable.
 The application is assumed to implement the snapshot/prefix law and atomic
 callbacks. This is not a model of a CRDT, a database, or arbitrary app merges.

 Commit, retention-floor advance, beginSnapshot, preparePush, applyPage and
 installSnapshot are atomic. Preparation COMPUTATION is separate: its commit
 rechecks the durable barrier. Crash preserves durable state, including the
 attempt's fixed pending set and barrier; only volatile work is discarded.
 Snapshot cut, publication (including op receipts), page assembly, and install
 are separate actions. Immutable page boundaries are chosen nondeterministically
 at the cut. Pull pages can have every legal size and keep a fixed target.

 s.log is also a ghost history oracle: GC makes its prefix inaccessible to
 FetchPull, but retains it for invariants and the server's application state.
 Ages saturate, keeping time finite while allowing unlimited ticks. The small
 tick constants test only the ORDERING snapshot lifetime < journal retention,
 not the real 600-second / 24-hour / 30-day durations or performance.

 We start with an already verified empty base at cursor 0. Real bootstrap must
 establish a base first. Matching author/hash, valid context/session tokens,
 successful storage/reconciliation, and fully applicable events are assumed.
 Abandon/recreate clears staged responses, abstracting the context check that
 discards stale replies. No explicit unbounded attempt IDs or packet pool.
 Rejections, cancel, noop semantics, auth, epoch changes, arbitrary payloads,
 quotas, idle-TTL refresh, fairness, and liveness are outside this model.
 A TLC pass proves only this finite instance; there is no cutoff theorem.
***************************************************************************)

CONSTANTS RetainTicks, SnapshotTicks, LoseBarrierOnCrash, SkipEventOnPull
ASSUME /\ RetainTicks \in Nat /\ SnapshotTicks \in Nat
       /\ 0 < SnapshotTicks /\ SnapshotTicks < RetainTicks
       /\ LoseBarrierOnCrash \in BOOLEAN /\ SkipEventOnPull \in BOOLEAN

Foreign1 == "foreign-1"
Foreign2 == "foreign-2"
Own == "local-canonical"
Tokens == {Foreign1, Foreign2, Own}
MaxEvents == 3
MaxPosition == 4

VARIABLES s, c, snap, pull
vars == <<s, c, snap, pull>>

Payload(entries) == [i \in 1..Len(entries) |-> entries[i].token]
Through(p) == SelectSeq(s.log, LAMBDA e: e.at <= p)
Prefix(p) == Payload(Through(p))
Between(a, b) == SubSeq(Prefix(b), Len(Prefix(a)) + 1, Len(Prefix(b)))
Has(seq, token) == \E i \in 1..Len(seq): seq[i] = token
OwnPosition == IF Has(Payload(s.log), Own)
               THEN (CHOOSE e \in {s.log[i] : i \in 1..Len(s.log)}:
                           e.token = Own).at ELSE 0
Min(a, b) == IF a < b THEN a ELSE b

EmptyPull == [to |-> -1, page |-> -1]
EmptySnapshot == [phase |-> "none", barrier |-> FALSE, pending |-> FALSE,
                  cut |-> 0, image |-> <<>>, age |-> 0, edges |-> {},
                  read |-> -1, verdict |-> 0, ready |-> FALSE]
ActiveImage == snap.phase \in {"cut", "published"}
LiveImage == ActiveImage /\ snap.age < SnapshotTicks

Init ==
    /\ s = [log |-> <<>>, ages |-> <<>>, head |-> 0, floor |-> 0]
    /\ c = [cursor |-> 0, base |-> <<>>, local |-> "none", receipt |-> 0,
            candidate |-> FALSE, running |-> TRUE]
    /\ snap = EmptySnapshot
    /\ pull = EmptyPull

Commit(token) ==
    /\ s.head < MaxPosition
    /\ ~Has(Payload(s.log), token)
    /\ CASE token = Foreign1 -> TRUE
         [] token = Foreign2 -> Has(Payload(s.log), Foreign1)
         [] token = Own -> c.local = "prepared"
    /\ s' = [s EXCEPT !.log = Append(@, [at |-> s.head + 1, token |-> token]),
                       !.ages = Append(@, 0), !.head = @ + 1]
    /\ UNCHANGED <<c, snap, pull>>

\* A skipped sequence allocation can leave a trailing empty interval, too.
SkipPosition ==
    /\ s.head < MaxPosition /\ s.head - Len(s.log) < 1
    /\ s' = [s EXCEPT !.head = @ + 1]
    /\ UNCHANGED <<c, snap, pull>>

Tick ==
    /\ s' = [s EXCEPT !.ages = [i \in 1..Len(s.ages) |->
                                          Min(s.ages[i] + 1, RetainTicks)]]
    /\ snap' = [snap EXCEPT !.age = IF ActiveImage
                                    THEN Min(@ + 1, SnapshotTicks) ELSE @]
    /\ UNCHANGED <<c, pull>>

GarbageCollect ==
    /\ \E k \in 1..Len(s.log):
           /\ s.log[k].at > s.floor
           /\ \A i \in 1..k: s.ages[i] >= RetainTicks
           /\ s' = [s EXCEPT !.floor = s.log[k].at]
    /\ UNCHANGED <<c, snap, pull>>

SaveLocal ==
    /\ c.running /\ c.local = "none"
    /\ c' = [c EXCEPT !.local = "intent"]
    /\ UNCHANGED <<s, snap, pull>>

ComputePrepare ==
    /\ c.running /\ c.local = "intent" /\ ~snap.barrier /\ ~c.candidate
    /\ c' = [c EXCEPT !.candidate = TRUE]
    /\ UNCHANGED <<s, snap, pull>>

CommitPrepare ==
    /\ c.running /\ c.local = "intent" /\ c.candidate /\ ~snap.barrier
    /\ c' = [c EXCEPT !.local = "prepared", !.candidate = FALSE]
    /\ UNCHANGED <<s, snap, pull>>

\* A push receipt never advances the read cursor or removes the overlay.
ReceiveReceipt ==
    /\ c.running /\ OwnPosition > 0 /\ c.receipt = 0
    /\ c' = [c EXCEPT !.receipt = OwnPosition]
    /\ UNCHANGED <<s, snap, pull>>

BeginPull ==
    /\ c.running /\ snap.phase = "none" /\ pull.to = -1
    /\ pull' = [to |-> s.head, page |-> -1]
    /\ UNCHANGED <<s, c, snap>>

LegalPageEnd(p) ==
    \/ p = pull.to
    \/ /\ Has([i \in 1..Len(s.log) |-> s.log[i].at], p)
       /\ Len(Between(c.cursor, p)) > 0
       /\ Len(Between(p, pull.to)) > 0

FetchPull ==
    /\ c.running /\ pull.to >= 0 /\ pull.page = -1
    /\ c.cursor >= s.floor
    /\ \E p \in c.cursor..pull.to:
           /\ LegalPageEnd(p)
           /\ pull' = [pull EXCEPT !.page = p]
    /\ UNCHANGED <<s, c, snap>>

LosePullResponse ==
    /\ c.running /\ pull.page >= 0
    /\ pull' = [pull EXCEPT !.page = -1]
    /\ UNCHANGED <<s, c, snap>>

PullExpired ==
    /\ c.running /\ pull.to >= 0 /\ pull.page = -1 /\ c.cursor < s.floor
    /\ pull' = EmptyPull
    /\ UNCHANGED <<s, c, snap>>

ApplyPage ==
    /\ c.running /\ snap.phase = "none" /\ pull.page >= 0
    /\ LET events == Between(c.cursor, pull.page)
           applied == IF SkipEventOnPull /\ Len(events) > 0
                      THEN SubSeq(events, 2, Len(events)) ELSE events
       IN c' = [c EXCEPT !.base = @ \o applied, !.cursor = pull.page,
                        !.local = IF Has(events, Own) THEN "covered" ELSE @,
                        !.receipt = IF Has(events, Own) THEN OwnPosition ELSE @]
    /\ pull' = IF pull.page = pull.to THEN EmptyPull
               ELSE [pull EXCEPT !.page = -1]
    /\ UNCHANGED <<s, snap>>

BeginSnapshot ==
    /\ c.running /\ snap.phase = "none"
    /\ snap' = [EmptySnapshot EXCEPT !.phase = "begun", !.barrier = TRUE,
                                    !.pending = (c.local = "prepared")]
    /\ pull' = EmptyPull
    /\ UNCHANGED <<s, c>>

CutSnapshot ==
    /\ snap.phase = "begun"
    /\ \E edges \in SUBSET (0..Len(s.log)):
           /\ IF Len(s.log) = 0 THEN edges = {0}
              ELSE edges \subseteq 1..Len(s.log) /\ Len(s.log) \in edges
           /\ snap' = [snap EXCEPT !.phase = "cut", !.cut = s.head,
                                  !.image = Payload(s.log), !.edges = edges]
    /\ UNCHANGED <<s, c, pull>>

\* Receipt lookup is AFTER the cut and can observe acceptance P > S.
PublishSnapshot ==
    /\ snap.phase = "cut" /\ LiveImage
    /\ snap' = [snap EXCEPT !.phase = "published",
                           !.verdict = IF snap.pending THEN OwnPosition ELSE 0]
    /\ UNCHANGED <<s, c, pull>>

RequestSnapshotPage ==
    /\ c.running /\ snap.phase = "published" /\ LiveImage
    /\ snap.read < Len(snap.image) /\ ~snap.ready
    /\ snap' = [snap EXCEPT !.ready = TRUE]
    /\ UNCHANGED <<s, c, pull>>

LoseSnapshotResponse ==
    /\ c.running /\ snap.ready
    /\ snap' = [snap EXCEPT !.ready = FALSE]
    /\ UNCHANGED <<s, c, pull>>

ReceiveSnapshotPage ==
    /\ c.running /\ snap.ready /\ LiveImage
    /\ LET next == CHOOSE p \in snap.edges:
                         p > snap.read /\ \A q \in snap.edges:
                                                q > snap.read => p <= q
       IN snap' = [snap EXCEPT !.read = next, !.ready = FALSE]
    /\ UNCHANGED <<s, c, pull>>

InstallSnapshot ==
    /\ c.running /\ snap.phase = "published" /\ LiveImage
    /\ snap.read = Len(snap.image)
    /\ c' = [c EXCEPT !.base = snap.image, !.cursor = snap.cut,
                     !.local = IF snap.pending /\ snap.verdict > 0
                                   /\ snap.verdict <= snap.cut
                                THEN "covered" ELSE @,
                     !.receipt = IF snap.verdict > 0 THEN snap.verdict ELSE @,
                     !.candidate = FALSE]
    /\ snap' = EmptySnapshot
    /\ UNCHANGED <<s, pull>>

\* Same fixed attempt/pending, but a new image; old pages are discarded.
RecreateExpiredSnapshot ==
    /\ c.running /\ ActiveImage /\ snap.age = SnapshotTicks
    /\ snap' = [EmptySnapshot EXCEPT !.phase = "begun",
                                    !.barrier = snap.barrier,
                                    !.pending = snap.pending]
    /\ UNCHANGED <<s, c, pull>>

AbandonSnapshot ==
    /\ c.running /\ snap.phase # "none"
    /\ snap' = EmptySnapshot
    /\ c' = [c EXCEPT !.candidate = FALSE]
    /\ UNCHANGED <<s, pull>>

Crash ==
    /\ c.running
    /\ c' = [c EXCEPT !.running = FALSE, !.candidate = FALSE]
    /\ pull' = EmptyPull
    /\ snap' = [snap EXCEPT !.ready = FALSE,
                           !.barrier = IF LoseBarrierOnCrash THEN FALSE ELSE @]
    /\ UNCHANGED s

Restart ==
    /\ ~c.running
    /\ c' = [c EXCEPT !.running = TRUE]
    /\ UNCHANGED <<s, snap, pull>>

Next == (\E token \in Tokens: Commit(token))
        \/ SkipPosition \/ Tick \/ GarbageCollect
        \/ SaveLocal \/ ComputePrepare \/ CommitPrepare \/ ReceiveReceipt
        \/ BeginPull \/ FetchPull \/ LosePullResponse \/ PullExpired \/ ApplyPage
        \/ BeginSnapshot \/ CutSnapshot \/ PublishSnapshot
        \/ RequestSnapshotPage \/ LoseSnapshotResponse \/ ReceiveSnapshotPage
        \/ InstallSnapshot \/ RecreateExpiredSnapshot \/ AbandonSnapshot
        \/ Crash \/ Restart

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ s.log \in Seq([at: 1..MaxPosition, token: Tokens])
    /\ Len(s.log) <= MaxEvents /\ s.head \in 0..MaxPosition
    /\ s.floor \in 0..s.head
    /\ s.ages \in [1..Len(s.log) -> 0..RetainTicks]
    /\ c.cursor \in 0..s.head /\ c.base \in Seq(Tokens)
    /\ Len(c.base) <= MaxEvents
    /\ c.local \in {"none", "intent", "prepared", "covered"}
    /\ c.receipt \in 0..s.head
    /\ c.candidate \in BOOLEAN /\ c.running \in BOOLEAN
    /\ snap.phase \in {"none", "begun", "cut", "published"}
    /\ snap.barrier \in BOOLEAN /\ snap.pending \in BOOLEAN
    /\ snap.cut \in 0..s.head /\ snap.image \in Seq(Tokens)
    /\ Len(snap.image) <= MaxEvents /\ snap.age \in 0..SnapshotTicks
    /\ snap.edges \subseteq 0..Len(snap.image)
    /\ snap.read \in -1..Len(snap.image)
    /\ snap.verdict \in 0..s.head /\ snap.ready \in BOOLEAN
    /\ pull.to \in {-1} \cup (c.cursor..s.head)
    /\ pull.page \in {-1} \cup (c.cursor..pull.to)

\* Ordered canonical data AND its position move in one transaction.
ConfirmedPrefix == c.base = Prefix(c.cursor)

LocalCopies == Cardinality({i \in 1..Len(c.base): c.base[i] = Own})
               + (IF c.local \in {"intent", "prepared"} THEN 1 ELSE 0)
LocalConservation == LocalCopies = (IF c.local = "none" THEN 0 ELSE 1)

DurableBarrier == snap.barrier = (snap.phase # "none")
FixedPending == snap.phase # "none" /\ c.local = "prepared" => snap.pending
SnapshotSound == ActiveImage => snap.image = Prefix(snap.cut)
SnapshotCoverage == snap.phase = "published" /\ snap.pending
                    /\ Has(snap.image, Own) => snap.verdict = OwnPosition
ReceiptSound == /\ c.receipt > 0 => c.receipt = OwnPosition
                /\ snap.verdict > 0 => snap.verdict = OwnPosition
LiveSnapshotTailRetained == LiveImage => s.floor <= snap.cut

=============================================================================
