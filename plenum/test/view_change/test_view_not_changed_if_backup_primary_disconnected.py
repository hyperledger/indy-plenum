import pytest

from stp_core.loop.eventually import eventually
from plenum.test.conftest import txnPoolNodeSet, txnPoolNodesLooper
from plenum.test.helper import stopNodes, checkViewNoForNodes
from plenum.test.test_node import primaryNodeNameForInstance, nodeByName


def testViewNotChangedIfBackupPrimaryDisconnected(txnPoolNodeSet,
                                                  txnPoolNodesLooper, tconf):
    """
    View change does not occurs when backup's primary is disconnected
    """

    # Setup
    nodes = txnPoolNodeSet
    looper = txnPoolNodesLooper

    viewNoBefore = checkViewNoForNodes(nodes)
    primaryNodeForBackupInstance1Before = nodeByName(
        nodes, primaryNodeNameForInstance(nodes, 1))

    # Exercise
    stopNodes([primaryNodeForBackupInstance1Before], looper)

    # Verify
    remainingNodes = set(nodes) - {primaryNodeForBackupInstance1Before}

    looper.runFor(tconf.ToleratePrimaryDisconnection + 2)

    def assertNewPrimariesElected():
        with pytest.raises(AssertionError):
            assert checkViewNoForNodes(remainingNodes) == viewNoBefore + 1
        viewNoAfter = checkViewNoForNodes(remainingNodes, viewNoBefore)
        assert viewNoBefore == viewNoAfter

    looper.run(eventually(assertNewPrimariesElected, retryWait=1, timeout=30))
