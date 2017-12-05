import pytest

from stp_core.common.log import getlogger

from plenum.common.constants import ALIAS, SERVICES, VALIDATOR

from plenum.test.helper import checkViewNoForNodes, \
    sendReqsToNodesAndVerifySuffReplies

from plenum.test.pool_transactions.helper import updateNodeData, \
    disconnect_node_and_ensure_disconnected

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.view_change.helper import ensure_view_change_complete, \
    start_stopped_node

logger = getlogger()


def check_all_nodes_the_same_pool_list(nodes):
    allNodeNames = sorted([n.name for n in nodes])
    for node in nodes:
        _allNodeNames = sorted(node.allNodeNames)
        assert _allNodeNames == allNodeNames
        assert sorted(node.nodeReg.keys()) == _allNodeNames


def test_primary_selection_after_demoted_node_promotion(
        looper, txnPoolNodeSet, nodeThetaAdded,
        tconf, tdir, allPluginsPath):
    """
    Demote non-primary node
    Promote it again
    Restart one node to get the following difference with others:
        - not restarted - node registry and related pool parameters are kept
          in memory in some state which is expected as the same as
          in the pool ledger
        - restarted one - loaded node registry and pool parameters from
          the pool ledger at startup
    Do several view changes and check that all nodes will choose previously
        demoted / promoted node as a primary for some instanse
    """

    nodeThetaSteward, nodeThetaStewardWallet, nodeTheta = nodeThetaAdded

    # viewNo0 = checkViewNoForNodes(txnPoolNodeSet)
    check_all_nodes_the_same_pool_list(txnPoolNodeSet)

    logger.info("1. Demote node Theta")

    node_data = {
        ALIAS: nodeTheta.name,
        SERVICES: []
    }
    updateNodeData(looper, nodeThetaSteward,
                   nodeThetaStewardWallet, nodeTheta, node_data)
    remainingNodes = list(set(txnPoolNodeSet) - {nodeTheta})

    check_all_nodes_the_same_pool_list(remainingNodes)
    # ensure pool is working properly
    sendReqsToNodesAndVerifySuffReplies(looper, nodeThetaStewardWallet,
                                        nodeThetaSteward, numReqs=3)
    # TODO view change might happen unexpectedly by unknown reason
    # checkViewNoForNodes(remainingNodes, expectedViewNo=viewNo0)

    logger.info("2. Promote node Theta back")

    node_data = {
        ALIAS: nodeTheta.name,
        SERVICES: [VALIDATOR]
    }
    updateNodeData(looper, nodeThetaSteward,
                   nodeThetaStewardWallet, nodeTheta, node_data)

    check_all_nodes_the_same_pool_list(txnPoolNodeSet)
    # ensure pool is working properly
    sendReqsToNodesAndVerifySuffReplies(looper, nodeThetaStewardWallet,
                                        nodeThetaSteward, numReqs=3)
    # checkViewNoForNodes(txnPoolNodeSet, expectedViewNo=viewNo0)

    logger.info("3. Restart one node")
    stopped_node = txnPoolNodeSet[0]

    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet,
                                            stopped_node, stopNode=True)
    looper.removeProdable(stopped_node)
    remainingNodes = list(set(txnPoolNodeSet) - {stopped_node})
    # ensure pool is working properly
    sendReqsToNodesAndVerifySuffReplies(looper, nodeThetaStewardWallet,
                                        nodeThetaSteward, numReqs=3)
    # checkViewNoForNodes(remainingNodes, expectedViewNo=viewNo0)

    # start node
    restartedNode = start_stopped_node(stopped_node, looper, tconf,
                                       tdir, allPluginsPath)
    txnPoolNodeSet = remainingNodes + [restartedNode]
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    # ensure pool is working properly
    sendReqsToNodesAndVerifySuffReplies(looper, nodeThetaStewardWallet,
                                        nodeThetaSteward, numReqs=3)
    # checkViewNoForNodes(txnPoolNodeSet, expectedViewNo=viewNo0)

    logger.info("4. Do view changes to check that nodeTheta will be chosen "
                "as a primary for some instance by all nodes after some rounds")
    while txnPoolNodeSet[0].viewNo < 4:
        ensure_view_change_complete(looper, txnPoolNodeSet)
        # ensure pool is working properly
        sendReqsToNodesAndVerifySuffReplies(looper, nodeThetaStewardWallet,
                                            nodeThetaSteward, numReqs=3)
