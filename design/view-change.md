
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
  each new view-change cerficate is added new primary makes an attempt to
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
  
  There was a parapgraph in original papers stating that node that sent
  _VIEW-CHANGE_ message could be uncooperative and don't send it to some
  nodes. In that case since primary has a view-change certificate it is
  guaranteed that it is possible to get required _VIEW-CHANGE_ from primary
  and _f_ matching _VIEW-CHANGE-ACKS_ from non-faulty nodes. Most papers from
  original authors don't describe exact protocol for this case, but in
  [Miguel Castro master thesis](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/01/thesis-mcastro.pdf) 
  there is section 5.2 on solving problem of eventually delivering all 
  messages even if underlying network can randomly drop them. Their proposed 
  solution is to periodically broadcast status messages, so other nodes can 
  learn about missing messages and retransmit them.

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
  ledger, but this is _O(N)_ operation with yet undetermined bounds on _N_.
  
  Even if we come up with some acceptable workaround and still keep ppSeqNo 
  reset logic there is one more problem - PBFT view-change protocol allows 
  multiple consecutive view changes, keeps history of potentially ordered 
  requests in different views and algorithm it uses to select requests to 
  get into new view relies on ppSeqNo constantly growing.
  
  Given above two paragraphs it should be seriously considered that we stop
  resetting ppSeqNo when entering new view, otherwise it will take probably
  unacceptable amount of time to prove that modified algorithm is correct,
  or go on without such proof, which will seriously undermine confidence in 
  it.
