---------------------------- MODULE Recovery ----------------------------
EXTENDS Naturals, TLC
\* Bounded control/liveness projection. Read/Delivery own data safety.
\* After Heal, runnable work completes within Delay clock ticks.
\* Finite stale backlog, advancing time, stable storage/access/codec assumed.
CONSTANTS Lease, Lifetime, Pages, Delay, Backoff, ErrorThreshold, LegacyPause
ASSUME /\ Lease > Delay + 1 /\ Lifetime > (Pages + 2) * (Delay + 1)
       /\ Pages > 0 /\ Delay > 0 /\ Backoff > 0 /\ ErrorThreshold > 0
VARIABLES phase, slot, ttl, life, page, retry, failures, healthy, lateOld, age
vars == <<phase, slot, ttl, life, page, retry, failures, healthy, lateOld, age>>
Min(a,b) == IF a < b THEN a ELSE b
Dec(n) == IF n = 0 THEN 0 ELSE n - 1
Init == /\ phase = "ready" /\ slot = "old" /\ ttl = Lease /\ life = Lifetime
        /\ page = 0 /\ retry = 0 /\ failures = 0 /\ healthy = FALSE
        /\ lateOld = TRUE /\ age = 0
Heal == /\ ~healthy /\ healthy' = TRUE
        /\ UNCHANGED <<phase,slot,ttl,life,page,retry,failures,lateOld,age>>
\* A finite delayed request from abandoned A may renew/recreate A, not evict B.
LateA == /\ lateOld /\ lateOld' = FALSE
         /\ slot' = IF slot = "none" THEN "old" ELSE slot
         /\ ttl' = CASE slot = "none" -> Lease [] slot = "old" -> Min(Lease,life) [] OTHER -> ttl
         /\ life' = IF slot = "none" THEN Lifetime ELSE life
         /\ UNCHANGED <<phase,page,retry,failures,healthy,age>>
Runnable == /\ phase \in {"ready","pages","install"} /\ retry = 0
            /\ ~(phase = "ready" /\ slot = "old")
Tick == /\ (~healthy \/ ~Runnable \/ age < Delay)
        /\ LET expired == ttl <= 1 \/ life <= 1 IN
           /\ slot' = IF expired THEN "none" ELSE slot
           /\ ttl' = IF expired THEN 0 ELSE Dec(ttl)
           /\ life' = IF expired THEN 0 ELSE Dec(life)
        /\ retry' = Dec(retry)
        /\ age' = IF healthy /\ Runnable THEN age + 1 ELSE 0
        /\ UNCHANGED <<phase,page,failures,healthy,lateOld>>
Fail == /\ ~healthy /\ phase \in {"ready","pages"} /\ retry = 0
        /\ failures' = Min(ErrorThreshold,failures + 1)
        /\ phase' = IF LegacyPause /\ failures' = ErrorThreshold THEN "paused" ELSE phase
        /\ retry' = Backoff /\ age' = 0
        /\ UNCHANGED <<slot,ttl,life,page,healthy,lateOld>>
LostCreate == /\ ~healthy /\ phase = "ready" /\ slot = "none" /\ retry = 0
              /\ slot' = "mine" /\ ttl' = Lease /\ life' = Lifetime
              /\ failures' = Min(ErrorThreshold,failures + 1)
              /\ phase' = IF LegacyPause /\ failures' = ErrorThreshold THEN "paused" ELSE phase
              /\ retry' = Backoff /\ age' = 0 /\ UNCHANGED <<page,healthy,lateOld>>
Create == /\ healthy /\ phase = "ready" /\ retry = 0
          /\ IF slot = "old"
                THEN /\ retry' = Min(ttl,life)
                     /\ UNCHANGED <<phase,slot,ttl,life,page,failures>>
                ELSE /\ phase' = "pages" /\ page' = 0 /\ retry' = 0 /\ slot' = "mine"
                     /\ ttl' = IF slot = "none" THEN Lease ELSE Min(Lease,life)
                     /\ life' = IF slot = "none" THEN Lifetime ELSE life
                     /\ failures' = 0
          /\ age' = 0 /\ UNCHANGED <<healthy,lateOld>>
Fetch == /\ healthy /\ phase = "pages" /\ retry = 0
         /\ IF slot = "mine"
               THEN /\ page' = page + 1
                    /\ phase' = IF page' = Pages THEN "install" ELSE "pages"
                    /\ ttl' = Min(Lease,life)
               ELSE /\ phase' = "ready" /\ page' = 0 /\ UNCHANGED ttl
         /\ age' = 0 /\ failures' = 0 /\ UNCHANGED <<slot,life,retry,healthy,lateOld>>
Install == /\ phase = "install" /\ healthy /\ age' = 0
           /\ phase' = IF slot = "mine" THEN "done" ELSE "ready"
           /\ page' = IF slot = "mine" THEN page ELSE 0
           /\ UNCHANGED <<slot,ttl,life,retry,failures,healthy,lateOld>>
\* Application Resume is allowed, NOT assumed fair: no required human action.
Resume == /\ phase = "paused" /\ phase' = "ready" /\ failures' = 0 /\ age' = 0
          /\ UNCHANGED <<slot,ttl,life,page,retry,healthy,lateOld>>
Next == Heal \/ LateA \/ Tick \/ Fail \/ LostCreate \/ Create \/ Fetch \/ Install \/ Resume
Spec == Init /\ [][Next]_vars
        /\ WF_vars(Heal) /\ WF_vars(LateA) /\ WF_vars(Tick)
        /\ WF_vars(Create) /\ WF_vars(Fetch) /\ WF_vars(Install)
TypeOK == /\ phase \in {"ready","pages","install","done","paused"}
          /\ slot \in {"none","old","mine"} /\ ttl \in 0..Lease /\ life \in 0..Lifetime
          /\ page \in 0..Pages /\ retry \in 0..(Lease + Backoff)
          /\ failures \in 0..ErrorThreshold /\ age \in 0..Delay
          /\ healthy \in BOOLEAN /\ lateOld \in BOOLEAN
SlotConsistent == (slot = "none") <=> (ttl = 0 /\ life = 0)
CompleteHasAllPages == phase = "done" => page = Pages
EventualCompletion == <>(phase = "done")
=============================================================================
