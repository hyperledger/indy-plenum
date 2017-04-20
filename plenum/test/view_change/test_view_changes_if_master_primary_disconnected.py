from plenum.test.test_node import ensureElectionsDone
from stp_core.loop.eventually import eventually
from plenum.test.conftest import txnPoolNodeSet, txnPoolNodesLooper
from plenum.test.helper import stopNodes, checkViewNoForNodes, nodeByName, \
    primaryNodeNameForInstance


def testViewChangesIfMasterPrimaryDisconnected(pool_with_election_done,
                                               txnPoolNodesLooper):
    """
    View change occurs when master's primary is disconnected
    """

    # Setup
    nodes = pool_with_election_done
    looper = txnPoolNodesLooper

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
