from plenum.common.constants import ALIAS, SERVICES, VALIDATOR
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.conftest import whitelist
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    checkNodeDataForUnequality, checkNodeDataForEquality
from plenum.test.pool_transactions.helper import \
    updateNodeData
from stp_core.common.log import getlogger

logger = getlogger()


def test_catch_up_after_demoted(txnPoolNodeSet, nodeSetWithNodeAddedAfterSomeTxns):
    # 1. add a new node after sending some txns and check that catch-up
    # is done (the new node is up to date)
    looper, newNode, client, wallet, newStewardClient, \
    newStewardWallet = nodeSetWithNodeAddedAfterSomeTxns
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:4])

    # 2. turn the new node off (demote)
    node_data = {
        ALIAS: newNode.name,
        SERVICES: []
    }
    updateNodeData(looper, newStewardClient,
                   newStewardWallet, newNode,
                   node_data)

    # 3. send more requests, so that the new node's state is outdated
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)
    checkNodeDataForUnequality(newNode, *txnPoolNodeSet[:-1])

    # 4. turn the new node on
    node_data = {
        ALIAS: newNode.name,
        SERVICES: [VALIDATOR]
    }
    updateNodeData(looper, newStewardClient,
                   newStewardWallet, newNode,
                   node_data)

    # 5. make sure catch-up is done (the new node is up to date again)
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:-1])

    # 6. send more requests and make sure that the new node participates
    # in processing them
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 10)
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:-1])
