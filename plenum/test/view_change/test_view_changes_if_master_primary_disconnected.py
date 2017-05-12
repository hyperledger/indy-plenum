import pytest

from plenum.test.test_node import ensureElectionsDone, \
    primaryNodeNameForInstance, nodeByName, get_master_primary_node, \
    ensure_node_disconnected
from plenum.test import waits
from stp_core.loop.eventually import eventually
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper
from plenum.test.helper import stopNodes, checkViewNoForNodes, \
    sendReqsToNodesAndVerifySuffReplies


@pytest.mark.skip(reason='SOV-1020')
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

    # Stop primary
    stopNodes([old_pr_node], looper)
    looper.removeProdable(old_pr_node)
    remainingNodes = set(nodes) - {old_pr_node}
    # Sometimes it takes time for nodes to detect disconnection
    ensure_node_disconnected(looper, old_pr_node, remainingNodes, timeout=20)

    looper.runFor(tconf.ToleratePrimaryDisconnection + 2)

    def assertNewPrimariesElected():
        checkViewNoForNodes(remainingNodes, viewNoBefore + 1)
        new_pr_node = get_master_primary_node(remainingNodes)
        assert old_pr_node != new_pr_node

    # Give some time to detect disconnection and then verify that view has
    # changed and new primary has been elected
    looper.run(eventually(assertNewPrimariesElected, retryWait=1, timeout=90))
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
