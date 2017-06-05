from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data, \
    waitNodeDataUnequality
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper, nodeThetaAdded, \
    stewardAndWallet1, steward1, stewardWallet

from plenum.test.delayers import cDelay, delay_3pc_messages
from plenum.test.test_node import getNonPrimaryReplicas, \
    checkProtocolInstanceSetup
from plenum.test.view_change.helper import ensure_view_change


def test_slow_nodes_catchup_before_selecting_primary_in_new_view(looper,
                                                                 txnPoolNodeSet,
                                                                 steward1,
                                                                 stewardWallet,
                                                                 tconf):
    """
    Delay 3PC to 1 node and then cause view change so by the time the view
    change happens(each node gets >2f+1 `INSTANCE_CHANGE`s), the slow node is
    behind other nodes. The should initiate catchup to come to the same state
    as other nodes. Not sending client requests while catching up, that will
    be tested in another test.
    """
    nprs = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    slow_node = nprs[0].node
    fast_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    delay = tconf.PerfCheckFreq
    delay_3pc_messages([slow_node], 0, delay=delay)
    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, 20)
    ensure_all_nodes_have_same_data(looper, nodes=fast_nodes)
    waitNodeDataUnequality(looper, slow_node, *fast_nodes)

    catchup_start_counts = {n.name: n.spylog.count(n.ledgerManager.startCatchUpProcess)
                            for n in txnPoolNodeSet}
    catchup_done_counts = {n.name: n.spylog.count(n.allLedgersCaughtUp)
                            for n in txnPoolNodeSet}

    ensure_view_change(looper, txnPoolNodeSet)
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    # `slow_node` does catchup, `fast_nodes` don't
    for n in txnPoolNodeSet:
        if n == slow_node:
            assert n.spylog.count(n.ledgerManager.startCatchUpProcess) > catchup_start_counts[n.name]
            assert n.spylog.count(n.allLedgersCaughtUp) > catchup_done_counts[n.name]
        else:
            assert n.spylog.count(n.ledgerManager.startCatchUpProcess) == catchup_start_counts[n.name]
            assert n.spylog.count(n.allLedgersCaughtUp) == catchup_done_counts[n.name]

    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, 5)
