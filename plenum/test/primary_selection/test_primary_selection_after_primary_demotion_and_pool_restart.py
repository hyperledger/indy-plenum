from stp_core.common.log import getlogger

from plenum.common.constants import ALIAS, SERVICES
from plenum.test.pool_transactions.conftest import looper
from plenum.test.pool_transactions.helper import updateNodeData

from plenum.test.test_node import TestNode, checkNodesConnected, \
    ensureElectionsDone
from plenum.test.helper import checkViewNoForNodes, \
    sendReqsToNodesAndVerifySuffReplies

from plenum.test.primary_selection.helper import getPrimaryNodesIdxs
from plenum.common.config_helper import PNodeConfigHelper

logger = getlogger()

def test_primary_selection_after_primary_demotion_and_pool_restart(looper,
        txnPoolNodeSet, stewardAndWalletForMasterNode, txnPoolMasterNodes,
        tdir, tconf):
    """
    Demote primary and restart the pool.
    Pool should select new primary and have viewNo=0 after restart.
    """

    logger.info("1. turn off the node which has primary replica for master instanse")
    master_node = txnPoolMasterNodes[0]
    client, wallet = stewardAndWalletForMasterNode

    node_data = {
        ALIAS: master_node.name,
        SERVICES: []
    }
    updateNodeData(looper, client, wallet, master_node, node_data)

    restNodes = [node for node in txnPoolNodeSet if node.name != master_node.name]
    ensureElectionsDone(looper, restNodes)

    # ensure pool is working properly
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, numReqs=3)

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
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, numReqs=3)

    primariesIdxs = getPrimaryNodesIdxs(restNodes)
    assert restNodes[primariesIdxs[0]].name != master_node.name
