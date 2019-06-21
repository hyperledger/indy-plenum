
# PBFT based view change

## Basic requirements and assumptions

View change should not lose any request that was possibly ordered
by any node, and client possibly got a reply.

After view change all nodes should be in eventually consistent state.

Primaries are selected in round robin fashion, so primary for any given
viewNo could be found just from number of nodes and that viewNo.

It is assumed that any message sent will eventually be delivered, although
we don't place any upper limit on this time.

## Notations

- _i_, _j_ - node ID
- _v_ - viewNo
- _k_ - ppSeqNo of batch
- _d_ - digest of batch
- _h_ - ppSeqNo of stable checkpoint
- _L_ - log size
- _ck_ - checkpoint ppSeqNo
- _cd_ - checkpoint digest (state root in terms of indy plenum)
- _vd_ - view-change message digest

## PBFT view-change protocol description

- When some replica suspects that primary is malicious it enters view _v+1_.
  This means that it will stop processing messages from view _v_, which
  in turn ensures that we can rely on fact that if less than _n-f_ nodes
  acquired prepare certificate then request is not (and won't be) ordered.

  It is unclear from articles whether replica should stop processing 3PC
  messages from view _v+1_ or not (some state that it should stop processing
  all 3PC messages, some state nothing). Probably it is safe to stash all 3PC
  messages from _v+1_ until view-change is complete (and successful), but
  it could be that even this is unnecessary - if these messages are from
  malicious nodes then we won't get quorum anyways, and if they are from
  normal nodes that managed to complete view change earlier they won't be
  executed either until gaps are filled. Also, next step implies that during
  consecutive view changes we can acquire different pre-prepared requests from
  different views, which seemingly can happen only if we actually process
  pre-prepare messages from new view during view change.

- Each replica has sets _P_ and _Q_, containing tuples _(k, d, v)_, which are
  normally empty, but updated upon entering new view as follows:
  - for each _k_ in _h+1 .. h+L_:
    - if _k_ is prepared (has a prepared certificate) add _(k, d, v)_ to _P_,
      remove all other tuples with same _k_ from _P_
    - if _k_ is pre-prepared add _(k, d, v)_ to _Q_, remove all other tuples
      with same _k_ and _d_ from _Q_

  This means that _P_ can contain only one tuple for each distinct _k_, but
  _Q_ can contain multiple ones if different requests were pre-prepared
  with same _k_ in different views.

  After updating _P_ and _Q_ replica clears its log and multicasts message
  _VIEW-CHANGE(i, v+1, h, C, P, Q)_, where _C_ is a set of _(ck, cd)_ tuples.

- Upon receiving _VIEW-CHANGE_ message replica checks whether all tuples
  in _P_ and _Q_ are for view _v_ or less. If this is correct replica sends
  message _VIEW-CHANGE-ACK(j, i, v+1, vd)_ to new primary.

- New primary collects _VIEW-CHANGE_ + corresponding _n-f-2 VIEW-CHANGE-ACK_
  messages for each replica _i_, which (when considering _VIEW-CHANGE-ACK_
  which new primary could send to itself) form a view-change certificate, so
  it can be sure that _n-f_ replicas have same _VIEW-CHANGE_ message for
  replica _i_. Each view-change certificate is stored in _S_, and when
  each new view-change certificate is added new primary makes an attempt to
  construct _NEW-VIEW_ message. Authors of PBFT state that when new primary
  gets _n-f_ view-change certificates from normal replicas it is guaranteed
  that this attempt will succeed.

- Attempt to form a _NEW-VIEW_ message is as follows:
  - decide on checkpoint _(ck, cd)_ to use as a starting state for new view:
    - it should be largest known checkpoint
    - it should be contained in _C_ of _f+1_ replicas
    - it should be not less than _h_ of _n-f_ replicas
  - for each _k_ in _ck+1 .. ck+L_:
    - for each _(k, d, v)_ in any _P_ from any replica it should get into
      new view if:
      - there are _n-f_ replicas with _h < k_ in which for any _(k, d', v')_
        in _P_ _v' < v_ or _v'=v_, _d'=d_, and  
      - there are _f+1_ replicas with _Q_ containing _(k, d, v')_, _v' >= v_
    - if _P_ from _n-f_ replicas with _h < k_ doesn't contain request with
      this sequence number it was certainly not ordered and authors of PBFT
      suggest to assign _null_ request to _k_

  Since indy plenum performs dynamic validation of requests before sending
  or accepting pre-prepare this also should be done for all requests selected
  for ordering in new view. Those that fail validation were certainly not
  ordered in previous view so they should be also transformed to _null_
  requests. Also when doing this validation uncommited state gets updated.

  If we got requests for all _k_ in _ck+1 .. ck+L_ we can send _NEW-VIEW_
  message. Goal of original approach was to get as many requests to new view
  as possible, but if we are okay with discarding requests that were not
  commited then we could truncate request list for new view on first _null_
  request, since all later requests were certainly not commited either.

  Side note: authors of PBFT papers don't rely on dynamic validation during
  pre-prepare as they solve abstract problem of making sure that all replicas
  of state machine receive same events in same order. If we abstract state
  from BFT protocol we can just move dynamic validation to execution step and
  treat failures as yet another transition (which doesn't actually change
  ledger). Also this approach can potentially increase throughput because
  we'll no longer need to wait for all previous requests during pre-prepare
  phase before continuing ordering.

- When new primary decided on list of requests to get into new view it
  multicasts _NEW-VIEW(v+1, V, X)_ message, where _V_ is set of tuples
  _(i, vd)_ for each view-change certificate in _S_, and _X_ is a structure
  containing selected _(ck, cd)_ and list of requests to get into new view.

  Backups consider _NEW-VIEW_ message valid when they have correct matching
  _VIEW-CHANGE_ message for each entry in _V_ and agrees on contents of _X_.
  If backup finds that received _NEW-VIEW_ message as invalid it immediately
  enters view _v+2_, repeating all previous steps.

  There was a paragraph in original papers stating that node that sent
  _VIEW-CHANGE_ message could be uncooperative and don't send it to some
  nodes. In that case since primary has a view-change certificate it is
  guaranteed that it is possible to get required _VIEW-CHANGE_ from primary
  and _f_ matching _VIEW-CHANGE-ACK_ from non-faulty nodes. Most papers from
  original authors don't describe exact protocol for this case, but in
  [Miguel Castro master thesis](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/01/thesis-mcastro.pdf)
  there is section 5.2 on solving problem of eventually delivering all
  messages even if underlying network can randomly drop them. Their proposed
  solution is to periodically broadcast _STATUS-PENDING(i,h,v,l,N,V)_ message,
  in which _l_ is ppSeqNo of last executed request, _N_ is flag if replica
  has _NEW-VIEW_ message for view _v_, _V_ is set of flags indicating that
  replica accepted _VIEW-CHANGE_ message from replica _j_. Upon receiving
  _STATUS-PENDING_ message which indicates that replica _i_ did not receive
  _VIEW-CHANGE_ message from replica _j_:
  - replica _j_ should resend _VIEW-CHANGE_ to _i_ (if not malicious)
  - new primary should retransmit _j_'s _VIEW-CHANGE_ to _i_ (if available)
  - other replicas should resend _VIEW-CHANGE-ACK(i,j)_ to _i_ supporting
    _VIEW-CHANGE_ from primary (if they already received view-changes and
    acknowledged them to primary)

- After obtaining (or generating in case of new primary) correct _NEW-VIEW_
  message each replica sets stable checkpoint to _(ck, cd)_ from _X_ and
  records all requests in _X_ as pre-prepared. Backup replicas also broadcast
  _PREPARE_ messages for these requests. Since preparing (by primary) and
  checking (by backups) of _X_ includes update of uncommited state and
  dynamic validation of requests it is guaranteed that at this point all
  correct replicas will have same uncommited state corresponding to
  pre-prepared requests that got to new view. After that processing goes on
  in normal state. There are some caveats however.

  First one (also mentioned by authors of PBFT) is that it can happen so
  that some replica won't have checkpoint _(ck, cd)_ and it would require
  state transfer protocol to get to correct state. This could be done
  independent from normal request processing (they can still get prepare and
  commit certificates), which will be executed when state is caught up.
  There is already similar protocol implemented in indy plenum called
  catch up, hopefully it could be used as is in this situation.

  Another problem is that we need somehow to skip execution of requests that
  were ordered in previous view but view-change protocol started their
  processing again. Authors of PBFT rely on uniqueness of ppSeqNo of requests,
  so if node gets commit certificate for some request that it already executed
  it just skips it. However indy plenum implementation currently resets
  ppSeqNo counter upon entering new view, so it cannot be relied upon.
  One possible workaround is to check if request was already ordered using
  ledger, but to be implemented efficiently it needs some mapping from
  request digest to ledger state.

  Even if we solve above mentioned problem and still keep ppSeqNo reset
  logic there is one more thing to consider - PBFT view-change protocol
  allows multiple consecutive view changes, keeps history of potentially
  ordered requests in different views and algorithm it uses to select
  requests to get into new view relies on ppSeqNo constantly growing.
  Probably if we really stop processing of all 3PC messages during view
  change (as mentioned in the beginning of this section) we can guarantee
  that all requests pending to get into new view will be from same view,
  complex request selection algorithm can be simplified and view change
  algorithm will still work.

- All above is applied to just one instance of PBFT protocol, but RBFT is
  basically multiple PBFT instances running in parallel, with only master
  altering state. RBFT states that all instances on given node should start
  view change simultaneously and each instance should elect new primary
  independently. Since indy plenum implementation has simple round robin
  election it should be ok to just run view change on master with each backup
  just waiting for view change completion on master.

  This should not affect performance measurements (triggering another view
  change) because, given non-faulty primary on master replica:
  - it is guaranteed that primary on master replica will be the first to end
    view change and enter normal ordering state
  - therefore master instance will be the first one where new PRE-PREPARE
    messages will be generated
  - no COMMIT quorum could be gathered by any instance until _n-f_ nodes
    finish view change
  - so ordering (and performance measurements) by any instance will start only
    when _n-f_ nodes finish view change, and master instance will have at least
    as many pre-prepared requests as any other backup
  - therefore master instance performance will be not less than any backup
    instance in the beginning of new view

## Summary of main deviations from PBFT

1. Indy plenum resets ppSeqNo after each view change. This should either
   be fixed (which can break a lot of things in current code), or PBFT view
   change will need serious changes, like relying on digests to check if
   request was ordered and disabling all request processing during view
   change.

2. Indy plenum performs dynamic validation of requests during pre-prepare
   phase. This leads to changes in selection of requests that get into new
   view. Also original PBFT approach of treating each request independent
   from state seems more clean and probably more performant, so this could
   be considered in future.

3. Indy plenum doesn't implement null requests and probably won't support
   ordering requests with gaps. While it doesn't seem to pose a real problem
   authors of PBFT state that allowing to get as many requests into new view
   as possible (even if they were not commited in previous view) improves
   overall performance of system.

4. Indy plenum implements RBFT which basically is multiple PBFT instances,
   with only one (master) changing state. For simplicity view change protocol
   can be applied only to master instance, with backups synced to master
   during view change.

## Proposed deviations

1. Reset pp_seq_no to 1 with a new view
2. Order 3PC Batches from the last view (with a NEW_VIEW msg) with the previous view's viewNo. This is needed to have consistent audit ledger.


#### Plan of Attack

- NEW_VIEW needs to consist of a list of PrePrepares (with all the information such as txn and state roots). NEW_VIEW is not passed to Orderer, only the PrePrepares are passed.
- Reset uncommitted state once start the view change
- Introduce a new `wait_for_new_view` flag to distinguish old-way and new-way view changes

    - This will be a flag in a ConsensusDataProvider
    - The flag is set to TRUE once view change is started (and viewNo +=1)
    - The flag is set to FALSE once a valid NEW_VIEW is received

- There needs to be `last_prep_cert` field in ConsensusDataProvider: (last_view_no, prep_cert_pp_seq_no)
- Reset 3PC state (delete all prepares, prepares, commits) once a valid NEW_VIEW is received (that is new_view_change_in_progress is set to false)
- Add an optional `currentViewNo` field for Prepares and Commits

    - Existing `viewNo` means the view no when the 3PC batch belongs to (from the Audit Ledger point of view), that is the viewNo where the corresponding PrePrepare was initially created
    - A new `currentViewNo` field is the view no where the 3PC batch was actually ordered. It may be not equal to `viewNo` is this is a Prepare or Commit after for 3PC batches in NEW_VIEW which belong to the previous view (since we've done a view change).
    - `currentViewNo` is optional, and if it's absent, then it's equal to `viewNo`.
    - `currentViewNo` is set only in the case described below.

- Create a new `ReplicaValidator` class that mostly copies the existing one and performs validation as follows:

    - for 3PC messages (PrePrepare, Prepare, Commit):
        - If msg.inst_id is incorrect - DISCARD
        - If msg.pp_seq_no is incorrect - DISCARD
        - If catch-up is in progress - STASH
        - Check if this is a msg for the NEW_VIEW's last prepared certificate:
            - if (msg.view_no == self.last_prep_cert[0]) and (msg.current_view_no == self.view_no) and (msg.pp_seq_no <= self.last_prep_cert[1]) - PROCESS
                - This means that we order messages from the previous view regardless of watermarks (since they are already moved to [1; 300] with the new view)
        - If already ordered - DISCARD
        - Check if from old view:
            - if view_no < self.viewNo - DISCARD
        - Check if future view:
            - if view_no > self.viewNo - STASH
        - Check if view change in progress
            - if `new_view_change_in_progress` - STASH
        - Check for watermarks
            - if not (h< pp_seq_no < H) - DISCARD
        - Otherwise - PROCESS
    - For Checkpoint:
        - the same as in the current ReplicaValidator

- Make changes in Orderer service:

    - `is_next_pre_prepare` returns TRUE, if
        - PRE_PREPARE's pp_seq_no == 1 and self.last_prep_cert == self._last_pre_prepared[1]
    - Make sure that PREPAREs and COMMITs are sent for every 3PC Batch regardless if it's ordered or not
    - Set `currentViewNo` for PREPARESs and COMMITs if self.view_no != msg.view_no.
    - Do not re-apply PRE_PREPARE if it's already ordered (however send PREPARE and COMMIT)
    - Do not Order already ordered requests once a quorum of COMMITs is received
    
## Plenum 2.0 Architecture

In order to properly implement PBFT View Change and cover it by unit, integration and simulation tests, architecture changes are required.

We denote these changes Plenum 2.0. They include 2 steps:
- Split tightly coupled  monoliths into a number of loosely coupled micro-services, where View Changer is one of them.
- Split services between multiple processes so that
  - Every Replica is in a separate process and does equal amount of work
  - Independent services (such as Read Request Service and Catchup Seeder Service) are in sepearate processes 
  
Splitting of Replica may be not needed if RBFT is replaced by anothe protocol from PBFT family.   

See [Plenum 2.0 Architecture](plenum_2_0_architecture.md) and the corresponding diagrams for details:
 - [Plenum 2.0 Architecture Class Diagram](plenum_2_0_architecture_class.png)
 - [Plenum 2.0 Architecture Object Diagram](plenum_2_0_architecture_object.png)
 - [Plenum 2.0 Architecture Communication Diagram](plenum_2_0_communication.png)


## Implementation plan 


- Define Interfaces needed for View Change Service 
- Simulation tests for View Changer (no integration)
- Implement PBFT viewchanger service with most basic functionality 
- Extract and integrate ConsensusDataProvider from Replica
- Modify WriteReqManager to meet Executor interface needs (2 SP) - AS
- Extract Orderer service from Replica
- Extract Checkpointer service from Replica
- Integrate Orderer and Checkpointer services into existing code base 
- Enable full ordering of batches from last view that were already ordered, make execution on replicas that executed them no-op 
- Integrate and run simulation tests with Orderer, Checkpointer, Ledger 
- Implementation: Integrate PBFT viewchanger service into current codebase
- Integrate view change simulation tests into CI 
- Debug: Integrate PBFT viewchanger service into current codebase
- Document PBFT view change protocol
- Load testing


