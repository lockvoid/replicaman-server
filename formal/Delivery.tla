----------------------------- MODULE Delivery -----------------------------
EXTENDS Naturals, FiniteSets, TLC

(*
Bounded safety model of docs/PROTOCOL.md and docs/WIRE.md push and cancel.
Receive is an ATOMIC transaction: authorization, receipt, application effect,
and acceptance/event commit share the same serialization boundary. This is an
assumption about the implementation, not something this model proves.

Requests may arrive in ANY order, and the same Receive remains enabled forever.
Replies are an unordered bounded set; a full set loses the new reply. Lose may
drop any reply; Deliver can deliver it repeatedly. Reply payloads are abstracted
to (epoch, subject, op): only the client context gate is modeled, not client
reconciliation, sessions, pull, snapshots, cursors, parsing, or external jobs.
There is no fairness assumption or eventual-delivery/completion claim.

The config uses TWO operations, TWO subjects, TWO hash values, TWO epochs, and
at most TWO distinct queued replies. Epochs never wrap. These bounds are NOT a
general cutoff theorem. Counts saturate at 2 (= two or more), so a second commit
is reachable and violates AtMostOnce instead of being forbidden by the model.
Operation labels are local to each modeled epoch; changing epoch does not
retag an old request. No real operation ID reuse by an honest client is implied.

checks is a safety observer. It compares actual pre/post durable state against
the request's pre-state permissions/identity; it does not prevent transitions.
In particular commits and effects count executions independently of receipts.
commits counts accepted operations/event appends; effects counts abstract
application mutations (an accepted noop has a commit but no application effect).

To test the deliberate missing-dedup mutant, use MutantNoDedupSpec instead of
Spec in a temporary config and check only AtMostOnce. TLC must find two pushes
for the same (epoch, op). No production behavior should use that specification.
*)

CONSTANTS Ops, Subjects, Hashes, LastEpoch, ReplyBound, EmptyValue
ASSUME /\ Cardinality(Ops) = 2
       /\ Cardinality(Subjects) = 2
       /\ Cardinality(Hashes) = 2
       /\ LastEpoch = 1
       /\ ReplyBound = 2

Epochs == 0..LastEpoch
None == EmptyValue
Accepted == {"applied", "noop"}
Outcomes == Accepted \cup {"rejected", "cancelled"}
Receipt == [subject : Subjects, hash : Hashes, outcome : Outcomes]
Requests == [epoch : Epochs, op : Ops, subject : Subjects,
             hash : Hashes, kind : {"push", "cancel"}]
ReplyTags == [epoch : Epochs, op : Ops, subject : Subjects]
CheckNames == {"epoch", "identity", "read", "write", "receipt"}

VARIABLES epoch, receipts, commits, effects, access, replies,
          clientEpoch, clientSubject, observed, checks

vars == <<epoch, receipts, commits, effects, access, replies,
          clientEpoch, clientSubject, observed, checks>>
durable == <<receipts, commits, effects>>
client == <<clientEpoch, clientSubject, observed>>

EmptyReceipts == [o \in Ops |-> None]
ZeroCounts == [o \in Ops |-> 0]
Bump(n) == IF n < 2 THEN n + 1 ELSE 2
CanRead(q) == access[q.subject] \in {"read", "write"}
CanWrite(q) == access[q.subject] = "write"
Changed(q) == IF receipts[q.op] = None THEN FALSE
              ELSE receipts[q.op].subject # q.subject
                   \/ receipts[q.op].hash # q.hash
Tag(q) == [epoch |-> q.epoch, op |-> q.op, subject |-> q.subject]

Init ==
    /\ epoch = 0
    /\ receipts = EmptyReceipts
    /\ commits = ZeroCounts
    /\ effects = ZeroCounts
    /\ access = [s \in Subjects |-> "write"]
    /\ replies = {}
    /\ clientEpoch = 0
    /\ clientSubject \in Subjects
    /\ observed = None
    /\ checks = [c \in CheckNames |-> TRUE]

Emit(q) == replies' = IF Cardinality(replies) < ReplyBound
                     THEN replies \cup {Tag(q)} ELSE replies

Audit(q) ==
    checks' = [
      epoch |-> checks.epoch /\
                (q.epoch # epoch => UNCHANGED durable),
      identity |-> checks.identity /\
                   (Changed(q) => UNCHANGED durable),
      read |-> checks.read /\ (~CanRead(q) => UNCHANGED durable),
      write |-> checks.write /\
                ((q.kind = "push" /\ ~CanWrite(q)) => UNCHANGED durable),
      receipt |-> checks.receipt /\
                  (\A o \in Ops : receipts[o] # None
                                   => receipts'[o] = receipts[o])]

Store(q, result) ==
    /\ receipts' = [receipts EXCEPT ![q.op] =
        [subject |-> q.subject, hash |-> q.hash, outcome |-> result]]
    /\ commits' = IF result \in Accepted
                  THEN [commits EXCEPT ![q.op] = Bump(@)] ELSE commits
    /\ effects' = IF result = "applied"
                  THEN [effects EXCEPT ![q.op] = Bump(@)] ELSE effects
    /\ Emit(q)

Receive(q, deduplicate) ==
    /\ IF q.epoch # epoch \/ ~CanRead(q) \/ Changed(q)
       THEN UNCHANGED <<durable, replies>>
       ELSE IF receipts[q.op] # None /\ deduplicate
       THEN /\ UNCHANGED durable
            /\ Emit(q)
       ELSE IF q.kind = "cancel"
       THEN Store(q, "cancelled")
       ELSE IF ~CanWrite(q)
       THEN UNCHANGED <<durable, replies>>
       ELSE \E result \in Accepted \cup {"rejected"} : Store(q, result)
    /\ Audit(q)
    /\ UNCHANGED <<epoch, access, client>>

SetAccess(s, permission) ==
    /\ access' = [access EXCEPT ![s] = permission]
    /\ UNCHANGED <<epoch, durable, replies, client, checks>>

Lose(r) ==
    /\ replies' = replies \ {r}
    /\ UNCHANGED <<epoch, durable, access, client, checks>>

Deliver(r) ==
    /\ observed' = IF r.epoch = clientEpoch /\ r.subject = clientSubject
                    THEN r ELSE observed
    /\ UNCHANGED <<epoch, durable, access, replies,
                    clientEpoch, clientSubject, checks>>

SwitchContext(s) ==
    /\ clientEpoch' = epoch
    /\ clientSubject' = s
    /\ observed' = None
    /\ UNCHANGED <<epoch, durable, access, replies, checks>>

NewEpoch ==
    /\ epoch < LastEpoch
    /\ epoch' = epoch + 1
    /\ receipts' = EmptyReceipts
    /\ commits' = ZeroCounts
    /\ effects' = ZeroCounts
    \* Old requests and replies remain possible; neither is relabeled.
    /\ UNCHANGED <<access, replies, client, checks>>

Next(deduplicate) ==
    \/ \E q \in Requests : Receive(q, deduplicate)
    \/ \E s \in Subjects, p \in {"none", "read", "write"} : SetAccess(s, p)
    \/ \E r \in replies : Lose(r) \/ Deliver(r)
    \/ \E s \in Subjects : SwitchContext(s)
    \/ NewEpoch

Spec == Init /\ [][Next(TRUE)]_vars
MutantNoDedupSpec == Init /\ [][Next(FALSE)]_vars

TypeOK ==
    /\ epoch \in Epochs
    /\ receipts \in [Ops -> Receipt \cup {None}]
    /\ commits \in [Ops -> 0..2]
    /\ effects \in [Ops -> 0..2]
    /\ access \in [Subjects -> {"none", "read", "write"}]
    /\ replies \subseteq ReplyTags
    /\ Cardinality(replies) <= ReplyBound
    /\ clientEpoch \in Epochs
    /\ clientSubject \in Subjects
    /\ observed \in ReplyTags \cup {None}
    /\ checks \in [CheckNames -> BOOLEAN]

AtMostOnce == \A o \in Ops : commits[o] <= 1
RejectedNoEffect == \A o \in Ops :
    IF receipts[o] = None THEN TRUE
    ELSE receipts[o].outcome \in {"rejected", "cancelled"}
         => commits[o] = 0 /\ effects[o] = 0
AtomicOutcome == \A o \in Ops :
    IF receipts[o] = None THEN commits[o] = 0 /\ effects[o] = 0
    ELSE IF receipts[o].outcome \in Accepted
         THEN /\ commits[o] > 0
              /\ effects[o] = IF receipts[o].outcome = "applied"
                              THEN commits[o] ELSE 0
         ELSE commits[o] = 0 /\ effects[o] = 0
OldEpochNoEffect == checks.epoch
ChangedIdentityNoEffect == checks.identity
AuthorizationNoEffect == checks.read /\ checks.write
ReceiptStable == checks.receipt
ContextIsolated == IF observed = None THEN TRUE
                   ELSE observed.epoch = clientEpoch
                        /\ observed.subject = clientSubject

\* The scalar labels have no ordering or distinguished role in this model.
Symmetry == Permutations(Ops) \cup Permutations(Subjects) \cup Permutations(Hashes)
=============================================================================
