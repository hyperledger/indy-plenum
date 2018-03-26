from plenum.test.spy_helpers import getAllReturnVals
from stp_core.loop.eventually import eventually

from plenum.common.constants import COMMIT, LEDGER_STATUS, MESSAGE_RESPONSE
from plenum.common.messages.node_messages import Commit
from plenum.common.util import check_if_all_equal_in_list
from plenum.test.delayers import cDelay, msg_rep_delay, lsDelay
from plenum.test.helper import sdk_send_batches_of_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import getNonPrimaryReplicas, ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change

TestRunningTimeLimitSec = 150


def test_reverted_unordered(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
    """
    Before starting catchup, revert any uncommitted changes to state and
    ledger. This is to avoid any re-application of requests that were
    ordered but stashed
    Example scenario
    prepared (1, 4)
    startViewChange
    start_catchup
    ...
    ....
    ...
    committed and send Ordered (1, 2)
    ...
    ....
    preLedgerCatchUp
    force_process_ordered, take out (1,2) and stash (1, 2)
    now process stashed Ordered(1,2), its requests will be applied again

    Simulation: Delay COMMITs to a node so that it can not order requests
    but has prepared them. Then trigger a view change and make sure the slow
    node has not ordered same number of requests as others but has prepared
    so it can order when it receives COMMITs while view change is in progress.
    The slow node should revert unordered batches and but it should eventually
    process the ordered requests, so delay LEDGER_STATUS too so catchup
    is delayed
    """
    slow_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1].node
    fast_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    slow_node.nodeIbStasher.delay(cDelay(120, 0))
    sent_batches = 5
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, 2 * sent_batches, sent_batches)

    # Fast nodes have same last ordered and same data
    last_ordered = [n.master_last_ordered_3PC for n in fast_nodes]
    assert check_if_all_equal_in_list(last_ordered)
    ensure_all_nodes_have_same_data(looper, fast_nodes)

    # Slow nodes have different last ordered than fast nodes
    assert last_ordered[0] != slow_node.master_last_ordered_3PC

    # Delay LEDGER_STATUS so catchup starts late
    slow_node.nodeIbStasher.delay(lsDelay(100))
    slow_node.nodeIbStasher.delay(msg_rep_delay(100))

    # slow_node has not reverted batches
    assert sent_batches not in getAllReturnVals(
        slow_node.master_replica,
        slow_node.master_replica.revert_unordered_batches)

    ensure_view_change(looper, txnPoolNodeSet)

    def chk1():
        # slow_node reverted all batches
        rv = getAllReturnVals(slow_node.master_replica,
                              slow_node.master_replica.revert_unordered_batches)
        assert sent_batches in rv

    looper.run(eventually(chk1, retryWait=1))

    # After the view change slow_node has prepared same requests as the fast
    # nodes have ordered
    assert last_ordered[0] == slow_node.master_replica.last_prepared_before_view_change

    # Deliver COMMITs
    slow_node.nodeIbStasher.reset_delays_and_process_delayeds(COMMIT)

    def chk2():
        # slow_node orders all requests as others have
        assert last_ordered[0] == slow_node.master_last_ordered_3PC

    looper.run(eventually(chk2, retryWait=1))

    # Deliver LEDGER_STATUS so catchup can complete
    slow_node.nodeIbStasher.reset_delays_and_process_delayeds(LEDGER_STATUS)
    slow_node.nodeIbStasher.reset_delays_and_process_delayeds(MESSAGE_RESPONSE)

    # Ensure all nodes have same data
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    def chk3():
        # slow_node processed stashed Ordered requests successfully
        rv = getAllReturnVals(slow_node,
                              slow_node.processStashedOrderedReqs)
        assert sent_batches in rv

    looper.run(eventually(chk3, retryWait=1))

    # Ensure pool is functional
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, 10, 2)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
