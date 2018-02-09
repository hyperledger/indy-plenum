from itertools import combinations

from plenum.common.constants import DOMAIN_LEDGER_ID, COMMIT
from plenum.test import waits
from plenum.test.delayers import cDelay, cr_delay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    check_last_ordered_3pc, assertEquality
from plenum.test.node_catchup.helper import waitNodeDataInequality, \
    make_a_node_catchup_twice, ensure_all_nodes_have_same_data
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.test_node import getNonPrimaryReplicas, \
    checkProtocolInstanceSetup
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually

Max3PCBatchSize = 2
TestRunningTimeLimitSec = 125

# Do not remove the next imports
from plenum.test.batching_3pc.conftest import tconf # noqa


def test_slow_node_reverts_unordered_state_during_catchup(looper,
                                                          txnPoolNodeSet,
                                                          client1,
                                                          wallet1,
                                                          client1Connected):
    """
    Delay COMMITs to a node such that when it needs to catchup, it needs to
    revert some unordered state. Also till this time the node should have
    receive all COMMITs such that it will apply some of the COMMITs (
    for which it has not received txns from catchup).
    For this delay COMMITs by long, do catchup for a little older than the
    state received in LedgerStatus, once catchup completes, reset delays and
    try to process delayed COMMITs, some COMMITs will be rejected but some will
    be processed since catchup was done for older ledger.
    """
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        3 * Max3PCBatchSize)
    nprs = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    slow_node = nprs[-1].node
    other_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    slow_master_replica = slow_node.master_replica

    commit_delay = 150
    catchup_rep_delay = 15

    # Delay COMMITs to one node
    slow_node.nodeIbStasher.delay(cDelay(commit_delay, 0))

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        6 * Max3PCBatchSize)
    ensure_all_nodes_have_same_data(looper, other_nodes)
    waitNodeDataInequality(looper, slow_node, *other_nodes)

    # Make the slow node receive txns for a smaller ledger so it still finds
    # the need to catchup
    delay_batches = 2
    make_a_node_catchup_twice(slow_node, other_nodes, DOMAIN_LEDGER_ID,
                              delay_batches * Max3PCBatchSize)

    def is_catchup_needed_count():
        return len(getAllReturnVals(slow_node, slow_node.is_catchup_needed,
                                    compare_val_to=True))
    old_lcu_count = slow_node.spylog.count(slow_node.allLedgersCaughtUp)
    old_cn_count = is_catchup_needed_count()

    # `slow_node` is slow to receive CatchupRep, so that it
    # gets a chance to order COMMITs
    slow_node.nodeIbStasher.delay(cr_delay(catchup_rep_delay))

    ensure_view_change(looper, txnPoolNodeSet)

    # Check last ordered of `other_nodes` is same
    for n1, n2 in combinations(other_nodes, 2):
        lst_3pc = check_last_ordered_3pc(n1, n2)

    def chk1():
        # `slow_node` has prepared all 3PC messages which
        # `other_nodes` have ordered
        assertEquality(slow_master_replica.last_prepared_before_view_change, lst_3pc)

    looper.run(eventually(chk1, retryWait=1))

    old_pc_count = slow_master_replica.spylog.count(
        slow_master_replica.can_process_since_view_change_in_progress)

    # Repair the network so COMMITs are received and processed
    slow_node.reset_delays_and_process_delayeds(COMMIT)

    def chk2():
        # COMMITs are processed for prepared messages
        assert slow_master_replica.spylog.count(
            slow_master_replica.can_process_since_view_change_in_progress) > old_pc_count

    looper.run(eventually(chk2, retryWait=1, timeout=5))

    def chk3():
        # Some COMMITs were ordered but stashed and they were processed
        rv = getAllReturnVals(slow_node, slow_node.processStashedOrderedReqs)
        assert delay_batches in rv

    looper.run(eventually(chk3, retryWait=1, timeout=catchup_rep_delay + 5))

    def chk4():
        # Catchup was done once
        assert slow_node.spylog.count(
            slow_node.allLedgersCaughtUp) > old_lcu_count

    looper.run(
        eventually(
            chk4,
            retryWait=1,
            timeout=waits.expectedPoolCatchupTime(
                len(txnPoolNodeSet))))

    def chk5():
        # Once catchup was done, need of other catchup was not found
        assertEquality(is_catchup_needed_count(), old_cn_count)

    looper.run(eventually(chk5, retryWait=1, timeout=5))

    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        2 * Max3PCBatchSize)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
