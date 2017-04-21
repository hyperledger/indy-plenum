from plenum.test.test_node import ensureElectionsDone, \
    primaryNodeNameForInstance, nodeByName
from stp_core.loop.eventually import eventually
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper
from plenum.test.helper import stopNodes, checkViewNoForNodes, \
    sendReqsToNodesAndVerifySuffReplies


def testViewChangesIfMasterPrimaryDisconnected(txnPoolNodeSet,
                                               looper, wallet1, client1,
                                               client1Connected):
    """
    View change occurs when master's primary is disconnected
    """

    # Setup
    nodes = txnPoolNodeSet

    viewNoBefore = checkViewNoForNodes(nodes)
    primaryNodeForMasterInstanceBefore = nodeByName(
        nodes, primaryNodeNameForInstance(nodes, 0))

    # Exercise
    stopNodes([primaryNodeForMasterInstanceBefore], looper)

    # Verify
    remainingNodes = set(nodes) - {primaryNodeForMasterInstanceBefore}

    def assertNewPrimariesElected():
        viewNoAfter = checkViewNoForNodes(remainingNodes)
        primaryNodeForMasterInstanceAfter = nodeByName(
            nodes, primaryNodeNameForInstance(remainingNodes, 0))
        assert viewNoBefore + 1 == viewNoAfter
        assert primaryNodeForMasterInstanceBefore != \
               primaryNodeForMasterInstanceAfter

    looper.run(eventually(assertNewPrimariesElected, retryWait=1, timeout=30))
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
