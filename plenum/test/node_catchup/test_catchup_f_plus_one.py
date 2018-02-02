from stp_core.common.log import getlogger
from plenum.common.config_helper import PNodeConfigHelper
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    waitNodeDataInequality, checkNodeDataForEquality
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist
from stp_core.types import HA

logger = getlogger()
txnCount = 5


def testNodeCatchupFPlusOne(txnPoolNodeSet, poolAfterSomeTxns, tconf, tdir,
                            tdirWithPoolTxns, allPluginsPath, testNodeClass):
    """
    Check that f+1 nodes is enough for catchup
    """
    looper, client, wallet = poolAfterSomeTxns

    assert len(txnPoolNodeSet) == 4

    node1 = txnPoolNodeSet[-1]
    node0 = txnPoolNodeSet[-2]

    logger.debug("Stopping node0 with pool ledger size {}".
                 format(node0.poolManager.txnSeqNo))
    disconnect_node_and_ensure_disconnected(
        looper, txnPoolNodeSet, node0, stopNode=True)
    looper.removeProdable(node0)

    logger.debug("Sending requests")
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)

    logger.debug("Stopping node1 with pool ledger size {}".
                 format(node1.poolManager.txnSeqNo))
    disconnect_node_and_ensure_disconnected(
        looper, txnPoolNodeSet, node1, stopNode=True)
    looper.removeProdable(node1)

    # Make sure new node got out of sync
    # Excluding state check since the node is stopped hence the state db is closed
    waitNodeDataInequality(looper, node0, *txnPoolNodeSet[:-2],
                           exclude_from_check=['check_state'])

    # TODO: Check if the node has really stopped processing requests?

    logger.debug("Starting the stopped node0")
    nodeHa, nodeCHa = HA(*node0.nodestack.ha), HA(*node0.clientstack.ha)
    config_helper = PNodeConfigHelper(node0.name, tconf, chroot=tdir)
    node0 = testNodeClass(node0.name,
                          config_helper=config_helper,
                          ha=nodeHa, cliha=nodeCHa,
                          config=tconf, pluginPaths=allPluginsPath)
    looper.add(node0)

    logger.debug("Waiting for the node0 to catch up")
    waitNodeDataEquality(looper, node0, *txnPoolNodeSet[:-2])

    logger.debug("Sending more requests")
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 2)
    checkNodeDataForEquality(node0, *txnPoolNodeSet[:-2])
