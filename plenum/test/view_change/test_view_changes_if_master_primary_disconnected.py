import pytest
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data

from plenum.test.test_node import get_master_primary_node, ensure_node_disconnected, ensureElectionsDone
from stp_core.loop.eventually import eventually
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper
from plenum.test.helper import stopNodes, checkViewNoForNodes, \
    sendReqsToNodesAndVerifySuffReplies, waitForViewChange


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
    remainingNodes = list(set(nodes) - {old_pr_node})
    # Sometimes it takes time for nodes to detect disconnection
    ensure_node_disconnected(looper, old_pr_node, remainingNodes, timeout=20)

    looper.runFor(tconf.ToleratePrimaryDisconnection + 2)

    # Give some time to detect disconnection and then verify that view has
    # changed and new primary has been elected
    waitForViewChange(looper, remainingNodes, viewNoBefore + 1)
    ensure_all_nodes_have_same_data(looper, nodes=remainingNodes)
    new_pr_node = get_master_primary_node(remainingNodes)
    assert old_pr_node != new_pr_node

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)

