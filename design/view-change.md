
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

## Rough roadmap

1. Implement PBFT view change as pluggable strategy that can cope with
   resetting ppSeqNo. This will be simplest possible variant with disabled
   request processing during view change, dropped requests that come after
   gaps and so on. Check that this implementation works. If it does,
   and performs better than current view change go ahead to we can take
   faster route A, otherwise move to route B.

Route A:

1. Set new view change as default, remove implementation of old view change
   and all related code, which should simplify next changes.

2. Fix current code so that it doesn't rely on resetting ppSeqNo after
   view change.

3. Implement other pieces of PBFT view change, which improve performance
   and/or pool responsiveness during view change.

Route B:

1. Fix current code so that it doesn't rely on resetting ppSeqNo after view
   change. Ensure that there're no regressions in current implementation of
   view change.

2. Implement critical pieces of PBFT view change that rely on ppSeqNo not
   being reset. Check that this implementation works. If it does, and performs
   better than current view change move to 3, otherwise additional analysis
   will be required.

3. Set new view change as default, remove implementation of old view change
   and all related code, which should simplify next changes.

4. Implement other pieces of PBFT view change, which improve performance
   and/or pool responsiveness during view change.

## Implementation details

Required changes in current code behaviour:
- request which is already ordered should not be dropped from consensus
  process, it should be ordered as if it is a new request but result in a no-op

Ideally all complex logic should be contained in separate testable classes:
- `Network` - interface for sending network messages and subscribing to them
- `Executor` - interface for requests dynamic validation and execution
- `Orderer` - implementation of normal requests ordering
- `Checkpointer` - implementation of checkpoints
- `Viewchanger` - implementation of view-change, depends on all above interfaces
  
Proper implementation details of `Network`, `Executor`, `Orderer`, and
`Checkpointer` are out of scope of this design. They are here to:
- provide interface seams to make implementing and testing `Viewchanger` easier
- provide better separation of state in replica
- make dependencies clear
- set basis for future refactorings

However in order to make `Viewchanger` work in current codebase these
interfaces still need to be implemented. To make changes as noninvasive as
possible they either should be made part of replica or implemented as thin
adaptors on top of replica. Interface descriptions here will be complete
from `Viewchanger` point of view, but some of them will need extension if
someone will need to refactor parts of node/replica into proper implementation
of `Orderer` and `Checkpointer`.

### Network

- `send(message)` - send message to network. This could be as simple as adding
  message to some outbox
- `subscribe([message_id], closure)` - subscribe to network messages, return
  subscription id. Question is still open whether this should be implemented as
  a simple callback which is called on every message (forcing each subscriber
  to implement their own routing logic) or as a different callbacks per each
  message type.
- `unsubscribe(id)` - unsubscribe from network messages
- `add_filter(predicate)` - stash or discard all incoming messages based on
  passed predicate (closure), return unique identifier
- `remove_filter(id)` - resume processing of all messages that were filtered
  by previously added predicate. Question is still open whether messages
  should be discarded or just stashed and re-executed on resume and how
  interface for this should be implemented.

### Executor

- `validate(batch)` - rewind (if needed) uncommited state to point where batch
  could be applied, check that batch is valid and apply it. If batch was
  already commited just accept it as valid. This probably should be split
  into several methods (like `is_commited`, `revert`, and so on), but this
  is to be determined during real work on code.

### Orderer

- `view_no()` - return current viewNo
- `enter_next_view()` - increment current viewNo, discard all previous messages
- `preprepared()` - return list of pre-prepared batches
- `prepared()` - return list of prepared batches
- `preprepare(batch)` - unconditionally put some batch into pre-prepared state
  producing all necessary side effects (like sending PREPARE messages from
  backup replicas)

Also it's possible that it will be easier to implement `batches()` method
that return list of batches in any state and then query them for their state
(pre-prepared, prepared, commited).

### Checkpointer

- `checkpoints()` - return list of all available checkpoints
- `stable_checkpoint()` - return current stable checkpoint
- `set_stable_checkpoint(h)` - update current stable checkpoint

### Viewchanger

- `__init__(network, executor, orderer, checkpointer)` - initialize viewchanger
  explicitly passing all dependencies
- `start_view_change()` - initiate view change protocol, called by node (or
  probably monitor) when we need to perform view change
- `is_view_change_in_progress()` - check if view change is in progress.
  Probably there also should be callback on view-change start/stop
- `process_time(timestamp)` - update internal time, possibly performing some
  actions. Called periodically by node, probably could be improved in future
  to use some abstract timer which can be subscribed to and set nearest needed
  timeout so it won't call tick unnecessarily. Main reason to inject timestamps
  externally is improved testability.
- `process_view_change()` - process _VIEW-CHANGE_ message
- `process_view_change_ack()` - process _VIEW-CHANGE-ACK_ message
- `process_new_view()` - process _NEW-VIEW_ message
- `process_status_pending()` - process _STATUS-PENDING_ message

## Implementation plan (minimal)

- enable full ordering of batches that were already ordered, make their
  execution on replicas that executed them no-op

  OR

  stop resetting ppSeqNo (and relying on this) in new view

- design executor interface taking into account current codebase so that
  it can be easily implemented

- implement viewchanger with most basic functionality using TDD, implementing
  mocks for network, executor, orderer and checkpointer as needed

- implement network, executor, orderer and checkpointer as adaptors for
  existing codebase

- integrate viewchanger into current codebase, make sure current integration
  tests pass
