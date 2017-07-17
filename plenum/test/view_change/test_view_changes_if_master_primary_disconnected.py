from plenum.test.delayers import icDelay
from stp_core.loop.eventually import eventually

from stp_core.types import HA

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data

from plenum.test.test_node import get_master_primary_node, ensure_node_disconnected, TestNode
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper
from plenum.test.helper import stopNodes, checkViewNoForNodes, \
    sendReqsToNodesAndVerifySuffReplies, waitForViewChange


def testViewChangesIfMasterPrimaryDisconnected(txnPoolNodeSet,
                                               looper, wallet1, client1,
                                               client1Connected, tconf,
                                               tdirWithPoolTxns, allPluginsPath):
    """
    View change occurs when master's primary is disconnected
    """

    # Setup
    nodes = txnPoolNodeSet

    old_view_no = checkViewNoForNodes(nodes)
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
    waitForViewChange(looper, remainingNodes, old_view_no + 1)
    ensure_all_nodes_have_same_data(looper, nodes=remainingNodes)
    new_pr_node = get_master_primary_node(remainingNodes)
    assert old_pr_node != new_pr_node

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)

    # Check if old primary can join the pool and still functions
    nodeHa, nodeCHa = HA(*old_pr_node.nodestack.ha), HA(*old_pr_node.clientstack.ha)
    old_pr_node = TestNode(old_pr_node.name, basedirpath=tdirWithPoolTxns, config=tconf,
                       ha=nodeHa, cliha=nodeCHa, pluginPaths=allPluginsPath)
    looper.add(old_pr_node)
    # Even after disconnection
    old_pr_node.nodeIbStasher.delay(icDelay(200))

    txnPoolNodeSet = remainingNodes + [old_pr_node]
    looper.run(eventually(checkViewNoForNodes,
                          txnPoolNodeSet, old_view_no + 1, timeout=10))
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
