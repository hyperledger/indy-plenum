from plenum.test.spy_helpers import get_count

from stp_core.loop.eventually import eventually
from plenum.common.types import HA
from stp_core.common.log import getlogger
from plenum.test.helper import check_last_ordered_3pc
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import checkNodesConnected, TestNode
from plenum.common.config_helper import PNodeConfigHelper

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()
txnCount = 5


def test_node_catchup_after_restart_no_txns(
        sdk_new_node_caught_up,
        txnPoolNodeSet,
        tdir,
        tconf,
        sdk_node_set_with_node_added_after_some_txns,
        tdirWithPoolTxns,
        allPluginsPath):
    """
    A node restarts but no transactions have happened while it was down.
    It would then use the `LedgerStatus` to catchup
    """
    looper, new_node, sdk_pool_handle, new_steward_wallet_handle = sdk_node_set_with_node_added_after_some_txns
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1],
                         exclude_from_check=['check_last_ordered_3pc_backup'])

    logger.debug("Stopping node {} with pool ledger size {}".
                 format(new_node, new_node.poolManager.txnSeqNo))
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, new_node)
    looper.removeProdable(name=new_node.name)

    logger.debug("Starting the stopped node, {}".format(new_node))
    nodeHa, nodeCHa = HA(*new_node.nodestack.ha), HA(*new_node.clientstack.ha)
    config_helper = PNodeConfigHelper(new_node.name, tconf, chroot=tdir)
    new_node = TestNode(
        new_node.name,
        config_helper=config_helper,
        config=tconf,
        ha=nodeHa,
        cliha=nodeCHa,
        pluginPaths=allPluginsPath)
    looper.add(new_node)
    txnPoolNodeSet[-1] = new_node
    looper.run(checkNodesConnected(txnPoolNodeSet))

    def chk():
        for node in txnPoolNodeSet[:-1]:
            check_last_ordered_3pc(new_node, node)

    looper.run(eventually(chk, retryWait=1))

    # sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1],
                         exclude_from_check=['check_last_ordered_3pc_backup'])
    # Did not receive any consistency proofs
    assert get_count(new_node.ledgerManager,
                     new_node.ledgerManager.processConsistencyProof) == 0
