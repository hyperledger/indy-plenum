from copy import copy

from plenum.common.constants import LAST_SENT_PRE_PREPARE
from plenum.test import waits
from plenum.test.checkpoints.conftest import chkFreqPatched
from plenum.test.helper import sdk_send_batches_of_random, assertExp
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, getPrimaryReplica, \
    checkNodesConnected, nodeByName
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually

CHK_FREQ = 2
LOG_SIZE = 3 * CHK_FREQ

nodeCount = 4

backup_inst_id = 1


def test_node_erases_last_sent_pp_key_on_pool_restart(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
        tconf, tdir, allPluginsPath, chkFreqPatched):

    # Get a node with a backup primary replica and the rest of the nodes
    replica = getPrimaryReplica(txnPoolNodeSet, instId=backup_inst_id)
    node = replica.node

    # Send some 3PC-batches and wait until the replica orders the 3PC-batches
    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               num_reqs=7, num_batches=7,
                               timeout=tconf.Max3PCBatchWait)

    looper.run(
        eventually(lambda: assertExp(replica.last_ordered_3pc == (0, 7)),
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

    # Check view no of the node and lastPrePrepareSeqNo of the replica
    assert node.viewNo == 0
    assert replica.lastPrePrepareSeqNo == 7
    assert replica.h == 6
    assert replica.H == 6 + LOG_SIZE

    # Ensure that there is a stored last sent PrePrepare key on the node
    assert LAST_SENT_PRE_PREPARE in node.nodeStatusDB

    # Restart all the nodes in the pool and wait for primary elections done
    all_nodes = copy(txnPoolNodeSet)
    for n in all_nodes:
        disconnect_node_and_ensure_disconnected(looper,
                                                txnPoolNodeSet,
                                                n.name,
                                                timeout=nodeCount,
                                                stopNode=True)
        looper.removeProdable(n)
        txnPoolNodeSet.remove(n)
    for n in all_nodes:
        txnPoolNodeSet.append(start_stopped_node(n, looper, tconf, tdir, allPluginsPath))
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureElectionsDone(looper, txnPoolNodeSet)

    node = nodeByName(txnPoolNodeSet, node.name)
    replica = node.replicas[backup_inst_id]

    # Verify that the node has erased the stored last sent PrePrepare key
    assert LAST_SENT_PRE_PREPARE not in node.nodeStatusDB

    # Verify correspondingly that after the pool restart the replica
    # (which must again be the primary in its instance) has not restored
    # lastPrePrepareSeqNo, not adjusted last_ordered_3pc and not shifted
    # the watermarks
    assert node.viewNo == 0
    assert replica.isPrimary
    assert replica.lastPrePrepareSeqNo == 0
    assert replica.last_ordered_3pc == (0, 0)
    assert replica.h == 0
    assert replica.H == 0 + LOG_SIZE

    # Send a 3PC-batch and ensure that the replica orders it
    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               num_reqs=1, num_batches=1,
                               timeout=tconf.Max3PCBatchWait)

    looper.run(
        eventually(lambda: assertExp(replica.last_ordered_3pc == (0, 1)),
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))
