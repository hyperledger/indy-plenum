from stp_core.loop.eventually import eventually
from plenum.test.conftest import txnPoolNodeSet, txnPoolNodesLooper
from plenum.test.helper import stopNodes, viewNoForNodes, \
    nodeByName, primaryNodeNameForInstance


def testViewChangesIfBackupPrimaryDisconnected(txnPoolNodeSet,
                                               txnPoolNodesLooper):

    # Setup
    nodes = set(txnPoolNodeSet)
    looper = txnPoolNodesLooper

    viewNoBefore = viewNoForNodes(nodes)
    primaryNodeForBackupInstance1Before = nodeByName(
        nodes, primaryNodeNameForInstance(nodes, 1))

    # Exercise
    stopNodes([primaryNodeForBackupInstance1Before], looper)

    # Verify
    remainingNodes = nodes - {primaryNodeForBackupInstance1Before}

    def assertNewPrimariesElected():
        viewNoAfter = viewNoForNodes(remainingNodes)
        primaryNodeForBackupInstance1After = nodeByName(
            nodes, primaryNodeNameForInstance(remainingNodes, 1))
        assert viewNoBefore + 1 == viewNoAfter
        assert primaryNodeForBackupInstance1Before != \
               primaryNodeForBackupInstance1After

    looper.run(eventually(assertNewPrimariesElected, retryWait=1, timeout=30))
