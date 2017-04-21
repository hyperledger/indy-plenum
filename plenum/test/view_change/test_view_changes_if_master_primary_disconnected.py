from plenum.test.test_node import ensureElectionsDone, \
    primaryNodeNameForInstance, nodeByName, get_master_primary_node
from stp_core.loop.eventually import eventually
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper
from plenum.test.helper import stopNodes, checkViewNoForNodes, \
    sendReqsToNodesAndVerifySuffReplies


def testViewChangesIfMasterPrimaryDisconnected(txnPoolNodeSet,
                                               looper, wallet1, client1,
                                               client1Connected, tconf):
    """
    View change occurs when master's primary is disconnected
    """

    # Setup
    nodes = txnPoolNodeSet

    viewNoBefore = checkViewNoForNodes(nodes)
    old_pr_node = get_master_primary_node(nodes)

    # Exercise
    stopNodes([old_pr_node], looper)
    # Verify
    remainingNodes = set(nodes) - {old_pr_node}

    looper.runFor(tconf.ToleratePrimaryDisconnection + 2)

    def assertNewPrimariesElected():
        checkViewNoForNodes(remainingNodes, viewNoBefore + 1)
        new_pr_node = get_master_primary_node(remainingNodes)
        assert old_pr_node != new_pr_node

    # Give some time to detect disconnection
    looper.run(eventually(assertNewPrimariesElected, retryWait=1, timeout=45))
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
