from time import perf_counter

from plenum.common.types import ThreePhaseType
from plenum.test import waits
from plenum.test.delayers import delay_3pc_messages, delayerMsgTuple
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    checkViewNoForNodes, sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.malicious_behaviors_node import slow_non_primary
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper
from plenum.test.test_node import getNonPrimaryReplicas, ensureElectionsDone, \
    getPrimaryReplica
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually


TestRunningTimeLimitSec = 150


def check_ordered(nodes, inst_id=0):
    ordered = [node.replicas[inst_id].ordered for node in nodes]
    assert ordered.count(ordered[0]) == len(ordered), ordered


def test_slow_node_start_view_same_pp_seq_no(txnPoolNodeSet, looper, wallet1,
                                             client1, client1Connected, tconf):
    """
    A slow node starts the new view with same pp_seq_no as other nodes
    """
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    delay = 3
    npr = slow_non_primary(txnPoolNodeSet, 0, delay)
    slow_node = npr.node
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
    new_view_no = ensure_view_change(looper, txnPoolNodeSet, client1, wallet1)
    assert new_view_no != old_view_no
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
    looper.run(eventually(check_ordered, txnPoolNodeSet, 0, retryWait=1,
                          timeout=3+delay))
    waitNodeDataEquality(looper, slow_node,
                         *[n for n in txnPoolNodeSet if n != slow_node])
    slow_node.resetDelays()
    slow_node.force_process_delayeds()

def test_slow_nodes_consistent_view(looper, txnPoolNodeSet, client1, wallet1,
                                    client1Connected):
    """
    Delay each node's request processing by a different amount but still nodes
    start in new view from same pp_seq_no and continue processing requests.
    :return:
    """
    min_delay = 3
    max_delay = 6

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 2)
    delay_3pc_messages(txnPoolNodeSet, 0, min_delay=min_delay,
                       max_delay=max_delay)
    reqs = sendRandomRequests(wallet1, client1, 5)
    ensure_view_change(looper, txnPoolNodeSet, client1, wallet1)
    reqs.extend(sendRandomRequests(wallet1, client1, 5))
    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs,
                                        customTimeoutPerReq=timeout+3*max_delay)    # Since each 3 phase message is delayed
    looper.run(eventually(check_ordered, txnPoolNodeSet, 0, retryWait=1,
                          timeout=5+max_delay))
    waitNodeDataEquality(looper, txnPoolNodeSet[0], *txnPoolNodeSet[1:])
    for node in txnPoolNodeSet:
        node.resetDelays()
        node.force_process_delayeds()


def test_fast_nodes_remove_non_ordered_messages(looper, txnPoolNodeSet, client1,
                                                wallet1, client1Connected):
    """
    2 non primaries are slow in receiving 3 phase messages, so after view
    change the fast ones remove 3 phase messages which will not be ordered in
    this view and revert state and ledger if they need to
    :return:
    """
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 2)
    delay = 3
    nprs = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    pr_replica = getPrimaryReplica(txnPoolNodeSet, 0)
    pr_node = pr_replica.node

    slow_nodes = [r.node for r in nprs[:2]]
    for node in slow_nodes:
        for typ in ThreePhaseType:
            node.nodeIbStasher.delay(delayerMsgTuple(delay, typ,
                                                     pr_node.name, 0))

    time_before_reqs = perf_counter()
    reqs = []
    for i in range(4):
        reqs.extend(sendRandomRequests(wallet1, client1, 10))
        looper.runFor(.01)

    ensure_view_change(looper, txnPoolNodeSet, client1, wallet1)
    ensureElectionsDone(looper, txnPoolNodeSet)
    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs,
                                        customTimeoutPerReq=timeout + 3*delay)
    looper.run(eventually(check_ordered, txnPoolNodeSet, 0, retryWait=1,
                          timeout=5 + 3 * delay))

    # The fast node (primary in an earlier view) would call revert
    later = []
    fast = [n.replicas[0] for n in txnPoolNodeSet if n not in slow_nodes]
    for r in fast:
        for e in r.spylog.getAll(r.revert.__name__):
            if e.starttime > time_before_reqs:
                later.append(True)
    assert later, 'No replica called revert'
