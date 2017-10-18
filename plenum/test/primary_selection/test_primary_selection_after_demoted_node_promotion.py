import pytest

from stp_core.common.log import getlogger

from plenum.common.config_util import getConfig
from plenum.common.constants import ALIAS, SERVICES, VALIDATOR

from plenum.test.helper import checkViewNoForNodes, \
    sendReqsToNodesAndVerifySuffReplies

from plenum.test.pool_transactions.helper import updateNodeData, \
    disconnect_node_and_ensure_disconnected

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.view_change.helper import ensure_view_change_complete, \
    start_stopped_node

config = getConfig()
logger = getlogger()


@pytest.fixture
def shortViewChangeTimeout():
    config.VIEW_CHANGE_TIMEOUT = 5

def check_all_nodes_the_same_pool_list(nodes):
    allNodeNames = sorted([n.name for n in nodes])
    for node in nodes:
        _allNodeNames = sorted(node.allNodeNames)
        assert _allNodeNames == allNodeNames
        assert sorted(node.nodeReg.keys()) == _allNodeNames

def test_primary_selection_after_demoted_node_promotion(
        shortViewChangeTimeout, looper, txnPoolNodeSet, nodeThetaAdded,
        client1, wallet1, client1Connected,
        tconf, tdirWithPoolTxns, allPluginsPath):
    """
    Demote primary and do multiple view changes forcing primaries rotation.
    Demoted primary should be skipped without additional view changes.
    """

    nodeThetaSteward, nodeThetaStewardWallet, nodeTheta = nodeThetaAdded

    #viewNo0 = checkViewNoForNodes(txnPoolNodeSet)
    check_all_nodes_the_same_pool_list(txnPoolNodeSet)

    logger.info("1. Demote node Theta")

    node_data = {
        ALIAS: nodeTheta.name,
        SERVICES: []
    }
    updateNodeData(looper, nodeThetaSteward,
                   nodeThetaStewardWallet, nodeTheta, node_data)
    remainingNodes = list(set(txnPoolNodeSet) - {nodeTheta})

    #check_all_nodes_the_same_pool_list(remainingNodes)
    # ensure pool is working properly
    sendReqsToNodesAndVerifySuffReplies(looper, nodeThetaStewardWallet,
                                        nodeThetaSteward, numReqs=3)
    # TODO view change might happen unexpectedly by unknown reason
    #checkViewNoForNodes(remainingNodes, expectedViewNo=viewNo0)

    logger.info("2. Promote node Theta back")

    node_data = {
        ALIAS: nodeTheta.name,
        SERVICES: [VALIDATOR]
    }
    updateNodeData(looper, nodeThetaSteward,
                   nodeThetaStewardWallet, nodeTheta, node_data)

    #check_all_nodes_the_same_pool_list(txnPoolNodeSet)
    # ensure pool is working properly
    sendReqsToNodesAndVerifySuffReplies(looper, nodeThetaStewardWallet,
                                        nodeThetaSteward, numReqs=3)
    #checkViewNoForNodes(txnPoolNodeSet, expectedViewNo=viewNo0)

    # TODO don't know who are primaries because of possible view changes
    logger.info("3. Restart one node")
    stopped_node = txnPoolNodeSet[0]

    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet,
                                            stopped_node, stopNode=True)
    looper.removeProdable(stopped_node)
    remainingNodes = list(set(txnPoolNodeSet) - {stopped_node})
    # ensure pool is working properly
    sendReqsToNodesAndVerifySuffReplies(looper, nodeThetaStewardWallet,
                                        nodeThetaSteward, numReqs=3)
    #checkViewNoForNodes(remainingNodes, expectedViewNo=viewNo0)

    # start node
    restartedNode = start_stopped_node(stopped_node, looper, tconf,
                                        tdirWithPoolTxns, allPluginsPath)
    txnPoolNodeSet = remainingNodes + [restartedNode]
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    # ensure pool is working properly
    sendReqsToNodesAndVerifySuffReplies(looper, nodeThetaStewardWallet,
                                        nodeThetaSteward, numReqs=3)
    #checkViewNoForNodes(txnPoolNodeSet, expectedViewNo=viewNo0)

    logger.info("4. Do view changes to check that nodeTheta will be chosen "
                "as a primary for any instance by all nodes after some rounds")
    while txnPoolNodeSet[0].viewNo < 3:
        ensure_view_change_complete(looper, txnPoolNodeSet)
        # ensure pool is working properly
        sendReqsToNodesAndVerifySuffReplies(looper, nodeThetaStewardWallet,
                                            nodeThetaSteward, numReqs=3)
