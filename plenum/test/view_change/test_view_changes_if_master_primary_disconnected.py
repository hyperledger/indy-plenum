from plenum.test import waits
from stp_core.loop.eventually import eventually
from plenum.test.conftest import txnPoolNodeSet, txnPoolNodesLooper
from plenum.test.helper import stopNodes, viewNoForNodes, nodeByName, \
    primaryNodeNameForInstance


def testViewChangesIfMasterPrimaryDisconnected(txnPoolNodeSet,
                                               txnPoolNodesLooper):

    # Setup
    nodes = set(txnPoolNodeSet)
    looper = txnPoolNodesLooper

    viewNoBefore = viewNoForNodes(nodes)
    primaryNodeForMasterInstanceBefore = nodeByName(
        nodes, primaryNodeNameForInstance(nodes, 0))

    # Exercise
    stopNodes([primaryNodeForMasterInstanceBefore], looper)

    # Verify
    remainingNodes = nodes - {primaryNodeForMasterInstanceBefore}

    def assertNewPrimariesElected():
        viewNoAfter = viewNoForNodes(remainingNodes)
        primaryNodeForMasterInstanceAfter = nodeByName(
            nodes, primaryNodeNameForInstance(remainingNodes, 0))
        assert viewNoBefore + 1 == viewNoAfter
        assert primaryNodeForMasterInstanceBefore != \
               primaryNodeForMasterInstanceAfter

    # TODO 20 is 'magic' timeout find the cause why the check fails with out it
    timeout = waits.expectedPoolInterconnectionTime(len(nodes)) +\
        waits.expectedPoolElectionTimeout(len(nodes)) + 20
    looper.run(eventually(assertNewPrimariesElected, retryWait=1, timeout=timeout))
