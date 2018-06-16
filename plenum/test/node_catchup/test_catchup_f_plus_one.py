from plenum.test.view_change.helper import start_stopped_node
from stp_core.common.log import getlogger
from plenum.common.config_helper import PNodeConfigHelper
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    waitNodeDataInequality, checkNodeDataForEquality
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist
from stp_core.types import HA

logger = getlogger()
txnCount = 5


def testNodeCatchupFPlusOne(looper,
                            txnPoolNodeSet,
                            sdk_pool_handle,
                            sdk_wallet_steward,
                            tconf, tdir,
                            tdirWithPoolTxns, allPluginsPath, testNodeClass):
    """
    Check that f+1 nodes is enough for catchup
    """

    assert len(txnPoolNodeSet) == 4

    node1 = txnPoolNodeSet[-1]
    node0 = txnPoolNodeSet[-2]

    logger.debug("Stopping node0 with pool ledger size {}".
                 format(node0.poolManager.txnSeqNo))
    disconnect_node_and_ensure_disconnected(
        looper, txnPoolNodeSet, node0, stopNode=True)

    logger.debug("Sending requests")
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 5)

    logger.debug("Stopping node1 with pool ledger size {}".
                 format(node1.poolManager.txnSeqNo))
    disconnect_node_and_ensure_disconnected(
        looper, txnPoolNodeSet, node1, stopNode=True)

    # Make sure new node got out of sync
    # Excluding state check since the node is stopped hence the state db is closed
    waitNodeDataInequality(looper, node0, *txnPoolNodeSet[:-2],
                           exclude_from_check=['check_state'])

    # TODO: Check if the node has really stopped processing requests?

    logger.debug("Starting the stopped node0")
    node0 = start_stopped_node(node0,
                               looper,
                               tconf,
                               tdir,
                               allPluginsPath=allPluginsPath)

    logger.debug("Waiting for the node0 to catch up")
    waitNodeDataEquality(looper, node0, *txnPoolNodeSet[:-2])

    logger.debug("Sending more requests")
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 2)
    checkNodeDataForEquality(node0, *txnPoolNodeSet[:-2])
