from itertools import combinations

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.batching_3pc.conftest import tconf
from plenum.test.delayers import cDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    check_last_ordered_3pc
from plenum.test.node_catchup.helper import waitNodeDataUnequality, \
    make_a_node_catchup_again, ensure_all_nodes_have_same_data
from plenum.test.test_node import getNonPrimaryReplicas, \
    checkProtocolInstanceSetup
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually

Max3PCBatchSize = 2


def test_slow_node_reverts_unordered_state_during_catchup(looper,
                                          txnPoolNodeSet,
                                          client1,
                                          wallet1,
                                          client1Connected):
    """
    Delay COMMITs to a node such that when it needs to catchup, it needs to
    revert some unordered state. Also till this time the node should have
    receive all COMMITs such that it will apply some of the COMMITs (
    for which it has not received txns from catchup). For this delay COMMITs by
    long, do catchup for a little older than the state received in
    LedgerStatus, once catchup completes, reset delays and try to process
    delayed COMMITs, some COMMITs will be rejected but some will be processed
    since catchup was done for older ledger.
    """
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        3 * Max3PCBatchSize)
    nprs = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    slow_node = nprs[-1].node
    other_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    slow_node.nodeIbStasher.delay(cDelay(150, 0))
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        6 * Max3PCBatchSize)
    waitNodeDataUnequality(looper, slow_node, *other_nodes)

    make_a_node_catchup_again(slow_node, other_nodes, DOMAIN_LEDGER_ID,
                              2*Max3PCBatchSize)

    ensure_view_change(looper, txnPoolNodeSet)

    for n1, n2 in combinations(other_nodes, 2):
        lst_3pc = check_last_ordered_3pc(n1, n2)

    def chk():
        assert slow_node.master_replica.last_prepared_before_view_change == lst_3pc
        assert slow_node.master_replica.peekitem(
            slow_node.master_replica.prepared_before_catchup, -1) == lst_3pc

    looper.run(eventually(chk, retryWait=1))
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
