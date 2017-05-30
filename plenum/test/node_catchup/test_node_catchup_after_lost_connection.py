from stp_core.common.log import getlogger
from plenum.test.test_node import ensure_node_disconnected
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality, waitNodeDataUnequality

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()
txnCount = 5


# TODO: Refactor tests to minimize module-scoped fixtures.They make tests depend on each other
def testNodeCatchupAfterLostConnection(newNodeCaughtUp, txnPoolNodeSet,
                                   nodeSetWithNodeAddedAfterSomeTxns):
    """
    A node that has poor internet connection and got unsynced after some transactions should eventually get the
    transactions which happened while it was not accessible
    :return:
    """
    looper, newNode, client, wallet, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    logger.debug("Stopping node {} with pool ledger size {}".
                 format(newNode, newNode.poolManager.txnSeqNo))
    looper.removeProdable(newNode)
    # TODO: Check if the node has really stopped processing requests?
    logger.debug("Sending requests")
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)
    # Make sure new node got out of sync
    waitNodeDataUnequality(looper, newNode, *txnPoolNodeSet[:-1])
    logger.debug("Ensure node {} gets disconnected".format(newNode))
    ensure_node_disconnected(looper, newNode, txnPoolNodeSet[:-1])
    logger.debug("Starting the stopped node, {}".format(newNode))
    looper.add(newNode)
    logger.debug("Waiting for the node to catch up, {}".format(newNode))
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:-1])
