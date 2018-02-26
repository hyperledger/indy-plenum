from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, \
    reconnect_node_and_ensure_connected
from plenum.test.test_node import ensure_node_disconnected
from stp_core.common.log import getlogger
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    waitNodeDataInequality, checkNodeDataForEquality

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()
txnCount = 5


# TODO: Refactor tests to minimize module-scoped fixtures.They make tests
# depend on each other
def testNodeCatchupAfterLostConnection(newNodeCaughtUp, txnPoolNodeSet,
                                       nodeSetWithNodeAddedAfterSomeTxns):
    """
    A node that has poor internet connection and got unsynced after some
    transactions should eventually get the transactions which happened while
    it was not accessible
    :return:
    """
    looper, newNode, client, wallet, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    logger.debug("Disconnecting node {}, ledger size {}".
                 format(newNode, newNode.domainLedger.size))
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, newNode,
                                            stopNode=False)

    # TODO: Check if the node has really stopped processing requests?
    logger.debug("Sending requests")
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)
    # Make sure new node got out of sync
    waitNodeDataInequality(looper, newNode, *txnPoolNodeSet[:-1])

    # logger.debug("Ensure node {} gets disconnected".format(newNode))
    ensure_node_disconnected(looper, newNode, txnPoolNodeSet[:-1])

    logger.debug("Connecting the node {} back, ledger size {}".
                 format(newNode, newNode.domainLedger.size))
    reconnect_node_and_ensure_connected(looper, txnPoolNodeSet, newNode)

    logger.debug("Waiting for the node to catch up, {}".format(newNode))
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:-1])

    logger.debug("Sending more requests")
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 10)
    checkNodeDataForEquality(newNode, *txnPoolNodeSet[:-1])
