------------------------------- MODULE Joint ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

(***************************************************************************
 Combined interference regression, NOT a composition/cutoff theorem.
 One stream, one initially saved local intent/op (bound to epoch 1), and ONE
 foreign event total. Two epochs and two local sessions, never reused. The
 client can change context ONCE: either replace its recovery session within
 epoch 1, or move to epoch 2 and archive old work. The server can advance epoch
 once independently. Executions with both client context changes are outside
 this bound. Old epoch logs/receipt are ghost history, never served anew.

 One response slot (receipt OR pull) and one snapshot slot can outlive context
 changes. At most one snapshot attempt per session; a second attempt waits for
 the old slot to be discarded. Push/cancel request tags form a set of at most
 four entries (two kinds x two sessions); processing the same entry repeatedly
 models retries. A full response slot loses the new reply. There are genuine
 retry/loss and crash/restart graph cycles, not permutations of one-shot steps.

 Ordered canonical event tokens form confirmed state. The local optimistic
 overlay and a recovery archive are separate. An archive may coexist with a
 canonical effect: retained recovery bytes are NOT a second visible effect.
 Accepted position is the immutable index of Own in epoch 1's ghost log.
 "cancelled" abbreviates rejected/cancelled; "unseen" is absence of a verdict.

 Snapshot begin/cut/publication/install and operation execution/delivery are
 separate. Publication freezes its then-current verdict; delivery may change
 the client's current verdict before install. Begin's preparation barrier and
 fixed pending set survive crashes. A computed prepare rechecks that barrier.
 Session/epoch changes preserve old response tags instead of relabeling them.

 Assumptions: atomic durable callbacks/commits, successful storage and archive,
 correct author/hash correlation, applicable canonical events, and application
 snapshot/prefix equivalence. Both modeled epochs have known empty initial
 application state; this does not establish real bootstrap correctness.
 Pull pages are materialized arbitrary nonempty prefixes; snapshots use one
 complete page. No GC, time, TTL, fixed-to traversal, quotas, payload parsing,
 auth, noop, multiple ops/clients, packet corruption, fairness, or liveness.
 The separate Read/Recovery models cover different bounded concerns; no formal
 refinement/composition between these models is asserted.

 audit is a sticky observer of ACTUAL pre/post values, never an action guard.
 It checks receipt stability and that delivery of a stale response changes no
 durable client/application/work state (discarding its transport slot is OK).
***************************************************************************)

CONSTANTS OverwriteWithUnseen, AcceptOldSession
ASSUME /\ OverwriteWithUnseen \in BOOLEAN /\ AcceptOldSession \in BOOLEAN

Epochs == 1..2
Sessions == 1..2
Own == "own-canonical"
Foreign == "foreign-canonical"
Tokens == {Own, Foreign}
Outcomes == {"unseen", "accepted", "rejected", "cancelled"}
Rejected == {"rejected", "cancelled"}
Requests == [session : Sessions, kind : {"push", "cancel"}]

VARIABLES s, c, w, sent, reply, snap, used, audit
vars == <<s, c, w, sent, reply, snap, used, audit>>
Has(xs, value) == \E i \in 1..Len(xs): xs[i] = value
Prefix(epoch, pos) == SubSeq(s.logs[epoch], 1, pos)
Fresh(tag) == tag.epoch = c.epoch /\ tag.session = c.session
Allowed(tag) == tag.epoch = c.epoch
                /\ (tag.session = c.session \/ AcceptOldSession)
EmptyReply == [kind |-> "none", epoch |-> 1, session |-> 1,
               outcome |-> "unseen", from |-> 0, to |-> 0, items |-> <<>>]
EmptySnapshot == [phase |-> "none", epoch |-> 1, session |-> 1,
                  pending |-> FALSE, cut |-> 0, image |-> <<>>,
                  outcome |-> "unseen"]
Reconcile(outcome, covered) ==
    [w EXCEPT !.known = outcome,
              !.pending = @ /\ outcome \notin Rejected /\ ~covered,
              !.archived = @ \/ (w.pending /\ outcome \in Rejected)]

Init ==
    /\ s = [epoch |-> 1, logs |-> [e \in Epochs |-> <<>>],
            receipt |-> "unseen"]
    /\ c = [epoch |-> 1, session |-> 1, cursor |-> 0, base |-> <<>>,
            barrier |-> FALSE, candidate |-> FALSE, up |-> TRUE]
    /\ w = [prepared |-> FALSE, pending |-> TRUE, archived |-> FALSE,
            known |-> "unseen"]
    /\ sent = {} /\ used = {}
    /\ reply = EmptyReply /\ snap = EmptySnapshot
    /\ audit = [receipt |-> TRUE, knowledge |-> TRUE, stale |-> TRUE]

ComputePrepare ==
    /\ c.up /\ c.epoch = 1 /\ ~c.barrier /\ ~c.candidate
    /\ w.pending /\ ~w.prepared
    /\ c' = [c EXCEPT !.candidate = TRUE]
    /\ UNCHANGED <<s, w, sent, reply, snap, used>>

CommitPrepare ==
    /\ c.up /\ c.epoch = 1 /\ ~c.barrier /\ c.candidate
    /\ w.pending /\ ~w.prepared
    /\ c' = [c EXCEPT !.candidate = FALSE]
    /\ w' = [w EXCEPT !.prepared = TRUE]
    /\ UNCHANGED <<s, sent, reply, snap, used>>

Send(kind) ==
    /\ c.up /\ c.epoch = 1 /\ w.prepared /\ w.pending
    /\ sent' = sent \cup {[session |-> c.session, kind |-> kind]}
    /\ UNCHANGED <<s, c, w, reply, snap, used>>

\* The two competing request kinds serialize at this shared transaction.
Handle(q, outcome) ==
    /\ q \in sent /\ s.epoch = 1
    /\ IF s.receipt # "unseen" THEN outcome = s.receipt
       ELSE IF q.kind = "cancel" THEN outcome = "cancelled"
       ELSE outcome \in {"accepted", "rejected"}
    /\ s' = [s EXCEPT !.receipt = outcome,
                     !.logs[1] = IF s.receipt = "unseen" /\ outcome = "accepted"
                                 THEN Append(@, Own) ELSE @]
    /\ reply' = IF reply.kind # "none" THEN reply
                ELSE [EmptyReply EXCEPT !.kind = "receipt",
                         !.session = q.session, !.outcome = outcome]
    /\ UNCHANGED <<c, w, sent, snap, used>>

ForeignCommit ==
    /\ \A e \in Epochs: ~Has(s.logs[e], Foreign)
    /\ s' = [s EXCEPT !.logs[s.epoch] = Append(@, Foreign)]
    /\ UNCHANGED <<c, w, sent, reply, snap, used>>

FetchPull ==
    /\ c.up /\ ~c.barrier /\ c.epoch = s.epoch /\ reply.kind = "none"
    /\ LET head == Len(s.logs[c.epoch])
       IN \E last \in (IF head = c.cursor THEN {head}
                        ELSE (c.cursor + 1)..head):
            reply' = [EmptyReply EXCEPT !.kind = "pull", !.epoch = c.epoch,
                      !.session = c.session, !.from = c.cursor, !.to = last,
                      !.items = SubSeq(s.logs[c.epoch], c.cursor + 1, last)]
    /\ UNCHANGED <<s, c, w, sent, snap, used>>

BeginSnapshot ==
    /\ c.up /\ snap.phase = "none" /\ c.session \notin used
    /\ c' = [c EXCEPT !.barrier = TRUE]
    /\ snap' = [EmptySnapshot EXCEPT !.phase = "begun", !.epoch = c.epoch,
                      !.session = c.session,
                      !.pending = (c.epoch = 1 /\ w.prepared /\ w.pending)]
    /\ used' = used \cup {c.session}
    /\ UNCHANGED <<s, w, sent, reply>>

CutSnapshot ==
    /\ snap.phase = "begun" /\ snap.epoch = s.epoch
    /\ snap' = [snap EXCEPT !.phase = "cut",
                           !.cut = Len(s.logs[snap.epoch]),
                           !.image = s.logs[snap.epoch]]
    /\ UNCHANGED <<s, c, w, sent, reply, used>>

PublishSnapshot ==
    /\ snap.phase = "cut" /\ snap.epoch = s.epoch
    /\ snap' = [snap EXCEPT !.phase = "published",
                        !.outcome = IF snap.pending THEN s.receipt ELSE "unseen"]
    /\ UNCHANGED <<s, c, w, sent, reply, used>>

ArchivePending ==
    /\ c.up /\ w.pending
    /\ w' = [w EXCEPT !.pending = FALSE, !.archived = TRUE]
    /\ c' = [c EXCEPT !.candidate = FALSE]
    /\ UNCHANGED <<s, sent, reply, snap, used>>

ReplaceSession ==
    /\ c.up /\ c.session = 1
    /\ c' = [c EXCEPT !.session = 2, !.barrier = FALSE, !.candidate = FALSE]
    /\ UNCHANGED <<s, w, sent, reply, snap, used>>

NewServerEpoch ==
    /\ s.epoch = 1
    /\ s' = [s EXCEPT !.epoch = 2]
    /\ UNCHANGED <<c, w, sent, reply, snap, used>>

ObserveNewEpoch ==
    /\ c.up /\ c.session = 1 /\ c.epoch = 1 /\ s.epoch = 2
    /\ c' = [c EXCEPT !.epoch = 2, !.session = 2, !.cursor = 0,
                     !.base = <<>>, !.barrier = FALSE, !.candidate = FALSE]
    /\ w' = [w EXCEPT !.pending = FALSE, !.archived = TRUE]
    /\ UNCHANGED <<s, sent, reply, snap, used>>

LoseReply ==
    /\ reply.kind # "none" /\ reply' = EmptyReply
    /\ UNCHANGED <<s, c, w, sent, snap, used>>

DiscardOldSnapshot ==
    /\ snap.phase # "none" /\ ~Fresh(snap) /\ snap' = EmptySnapshot
    /\ UNCHANGED <<s, c, w, sent, reply, used>>

Crash ==
    /\ c.up /\ c' = [c EXCEPT !.up = FALSE, !.candidate = FALSE]
    /\ UNCHANGED <<s, w, sent, reply, snap, used>>

Restart ==
    /\ ~c.up /\ c' = [c EXCEPT !.up = TRUE]
    /\ UNCHANGED <<s, w, sent, reply, snap, used>>

DeliverReceipt ==
    /\ c.up /\ reply.kind = "receipt"
    /\ w' = IF Allowed(reply) THEN Reconcile(reply.outcome, FALSE) ELSE w
    /\ reply' = EmptyReply
    /\ UNCHANGED <<s, c, sent, snap, used>>

DeliverPull ==
    /\ c.up /\ reply.kind = "pull"
    /\ IF Allowed(reply) /\ ~c.barrier /\ reply.from = c.cursor
       THEN /\ c' = [c EXCEPT !.base = @ \o reply.items, !.cursor = reply.to]
            /\ w' = IF reply.epoch = 1 /\ Has(reply.items, Own)
                    THEN Reconcile("accepted", TRUE) ELSE w
       ELSE UNCHANGED <<c, w>>
    /\ reply' = EmptyReply
    /\ UNCHANGED <<s, sent, snap, used>>

InstallSnapshot ==
    /\ c.up /\ snap.phase = "published"
    /\ IF Allowed(snap)
       THEN /\ c' = [c EXCEPT !.base = snap.image, !.cursor = snap.cut,
                             !.barrier = FALSE, !.candidate = FALSE]
            /\ LET merged == IF ~snap.pending THEN w.known
                             ELSE IF snap.outcome = "unseen" /\ ~OverwriteWithUnseen
                                  THEN w.known ELSE snap.outcome
               IN w' = Reconcile(merged, merged = "accepted"
                                        /\ snap.epoch = 1 /\ Has(snap.image, Own))
       ELSE UNCHANGED <<c, w>>
    /\ snap' = EmptySnapshot
    /\ UNCHANGED <<s, sent, reply, used>>

Audit(staleDelivery) ==
    audit' = [receipt |-> audit.receipt /\
                           (s.receipt # "unseen" => s'.receipt = s.receipt),
              knowledge |-> audit.knowledge /\
                           (w.known # "unseen" => w'.known = w.known),
              stale |-> audit.stale /\
                           (staleDelivery => <<c', w'>> = <<c, w>>)]

Other == ComputePrepare \/ CommitPrepare
         \/ (\E kind \in {"push", "cancel"}: Send(kind))
         \/ (\E q \in Requests, outcome \in Outcomes: Handle(q, outcome))
         \/ ForeignCommit \/ FetchPull
         \/ BeginSnapshot \/ CutSnapshot \/ PublishSnapshot
         \/ ArchivePending \/ ReplaceSession \/ NewServerEpoch \/ ObserveNewEpoch
         \/ LoseReply \/ DiscardOldSnapshot \/ Crash \/ Restart

Next == \/ (Other /\ Audit(FALSE))
        \/ (DeliverReceipt /\ Audit(~Fresh(reply)))
        \/ (DeliverPull /\ Audit(~Fresh(reply)))
        \/ (InstallSnapshot /\ Audit(~Fresh(snap)))
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ s.epoch \in Epochs /\ DOMAIN s.logs = Epochs
    /\ \A e \in Epochs: s.logs[e] \in Seq(Tokens) /\ Len(s.logs[e]) <= 2
    /\ s.receipt \in Outcomes
    /\ c.epoch \in Epochs /\ c.session \in Sessions
    /\ c.cursor \in 0..Len(s.logs[c.epoch]) /\ c.base \in Seq(Tokens)
    /\ Len(c.base) <= 2
    /\ c.barrier \in BOOLEAN /\ c.candidate \in BOOLEAN /\ c.up \in BOOLEAN
    /\ w.prepared \in BOOLEAN /\ w.pending \in BOOLEAN
    /\ w.archived \in BOOLEAN /\ w.known \in Outcomes
    /\ sent \subseteq Requests /\ used \subseteq Sessions
    /\ reply.kind \in {"none", "receipt", "pull"}
    /\ reply.epoch \in Epochs /\ reply.session \in Sessions
    /\ reply.outcome \in Outcomes /\ reply.items \in Seq(Tokens)
    /\ reply.from \in 0..2 /\ reply.to \in 0..2
    /\ snap.phase \in {"none", "begun", "cut", "published"}
    /\ snap.epoch \in Epochs /\ snap.session \in Sessions
    /\ snap.pending \in BOOLEAN /\ snap.outcome \in Outcomes
    /\ snap.cut \in 0..Len(s.logs[snap.epoch]) /\ snap.image \in Seq(Tokens)
    /\ audit \in [ {"receipt", "knowledge", "stale"} -> BOOLEAN ]

ConfirmedPrefix == c.base = Prefix(c.epoch, c.cursor)
WorkConserved == w.pending \/ w.archived \/ (c.epoch = 1 /\ Has(c.base, Own))
UnknownWorkPreserved == w.known = "unseen" => w.pending \/ w.archived
NoDoubleVisibleEffect == ~(w.pending /\ c.epoch = 1 /\ Has(c.base, Own))
RejectedNoEffect == /\ s.receipt \in Rejected => ~Has(s.logs[1], Own)
                    /\ w.known \in Rejected => ~w.pending /\ w.archived
OutcomeMatchesCommit == (s.receipt = "accepted") = Has(s.logs[1], Own)
CountOwn == Cardinality({i \in 1..Len(s.logs[1]): s.logs[1][i] = Own})
OwnAtMostOnce == CountOwn <= 1
KnownReceiptSound == w.known # "unseen" => w.known = s.receipt
ReceiptImmutable == audit.receipt
KnownReceiptNeverRegresses == audit.knowledge
OldResponseNoMutation == audit.stale
DurableBarrier == c.barrier = (snap.phase # "none" /\ Fresh(snap))
FixedPending == c.barrier /\ w.prepared /\ w.pending => snap.pending
SnapshotSound == snap.phase \in {"cut", "published"}
                 => snap.image = Prefix(snap.epoch, snap.cut)
EpochIsolation == /\ ~Has(s.logs[2], Own)
                  /\ c.epoch = 2 => ~w.pending /\ w.archived

=============================================================================
