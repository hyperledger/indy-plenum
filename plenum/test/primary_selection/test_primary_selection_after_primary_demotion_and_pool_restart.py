from plenum.common.util import hexToFriendly

from stp_core.common.log import getlogger

from plenum.test.pool_transactions.helper import sdk_send_update_node

from plenum.test.test_node import TestNode, checkNodesConnected, \
    ensureElectionsDone
from plenum.test.helper import checkViewNoForNodes, \
    sdk_send_random_and_check

from plenum.test.primary_selection.helper import getPrimaryNodesIdxs
from plenum.common.config_helper import PNodeConfigHelper

logger = getlogger()


def test_primary_selection_after_primary_demotion_and_pool_restart(looper,
                                                                   txnPoolNodeSet,
                                                                   sdk_pool_handle,
                                                                   sdk_wallet_steward,
                                                                   txnPoolMasterNodes,
                                                                   tdir, tconf):
    """
    Demote primary and restart the pool.
    Pool should select new primary and have viewNo=0 after restart.
    """

    logger.info("1. turn off the node which has primary replica for master instanse")
    master_node = txnPoolMasterNodes[0]
    node_dest = hexToFriendly(master_node.nodestack.verhex)
    sdk_send_update_node(looper, sdk_wallet_steward,
                         sdk_pool_handle,
                         node_dest, master_node.name,
                         None, None,
                         None, None,
                         services=[])

    restNodes = [node for node in txnPoolNodeSet if node.name != master_node.name]
    ensureElectionsDone(looper, restNodes)

    # ensure pool is working properly


    logger.info("2. restart pool")
    # Stopping existing nodes
    for node in txnPoolNodeSet:
        node.stop()
        looper.removeProdable(node)

    # Starting nodes again by creating `Node` objects since that simulates
    # what happens when starting the node with script
    restartedNodes = []
    for node in txnPoolNodeSet:
        config_helper = PNodeConfigHelper(node.name, tconf, chroot=tdir)
        restartedNode = TestNode(node.name,
                                 config_helper=config_helper,
                                 config=tconf, ha=node.nodestack.ha,
                                 cliha=node.clientstack.ha)
        looper.add(restartedNode)
        restartedNodes.append(restartedNode)

    restNodes = [node for node in restartedNodes if node.name != master_node.name]

    looper.run(checkNodesConnected(restNodes))
    ensureElectionsDone(looper, restNodes)
    checkViewNoForNodes(restNodes, 0)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 3)

    primariesIdxs = getPrimaryNodesIdxs(restNodes)
    assert restNodes[primariesIdxs[0]].name != master_node.name
