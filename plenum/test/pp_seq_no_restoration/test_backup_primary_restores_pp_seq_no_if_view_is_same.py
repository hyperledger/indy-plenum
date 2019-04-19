from common.serializers.serialization import node_status_db_serializer
from plenum.common.constants import LAST_SENT_PRE_PREPARE
from plenum.test import waits
from plenum.test.checkpoints.conftest import chkFreqPatched
from plenum.test.helper import sdk_send_batches_of_random, assertExp
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, getPrimaryReplica, \
    checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually

CHK_FREQ = 2
LOG_SIZE = 3 * CHK_FREQ

nodeCount = 4

backup_inst_id = 1


def test_backup_primary_restores_pp_seq_no_if_view_is_same(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
        tconf, tdir, allPluginsPath, chkFreqPatched, view_no):
    # Get a node with a backup primary replica
    replica = getPrimaryReplica(txnPoolNodeSet, instId=backup_inst_id)
    node = replica.node

    # Send some 3PC-batches and wait until the replica orders the 3PC-batches
    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               num_reqs=7, num_batches=7,
                               timeout=tconf.Max3PCBatchWait)

    seq_no = 7 if view_no == 0 else 8

    looper.run(
        eventually(lambda r: assertExp(r.last_ordered_3pc == (view_no, seq_no)),
                   replica,
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

    # Check view no of the node and lastPrePrepareSeqNo of the replica
    assert node.viewNo == view_no
    assert replica.lastPrePrepareSeqNo == seq_no

    # Ensure that the node has stored the last sent PrePrepare key
    assert LAST_SENT_PRE_PREPARE in node.nodeStatusDB
    last_sent_pre_prepare_key = \
        node_status_db_serializer.deserialize(
            node.nodeStatusDB.get(LAST_SENT_PRE_PREPARE))
    assert last_sent_pre_prepare_key == {str(backup_inst_id): [view_no, seq_no]}

    # Restart the node containing the replica
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node.name,
                                            stopNode=True)
    looper.removeProdable(node)
    txnPoolNodeSet.remove(node)

    node = start_stopped_node(node, looper, tconf, tdir, allPluginsPath)
    txnPoolNodeSet.append(node)

    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureElectionsDone(looper, txnPoolNodeSet)

    replica = node.replicas[backup_inst_id]

    # Verify that after the successful propagate primary procedure the replica
    # (which must still be the primary in its instance) has restored
    # lastPrePrepareSeqNo and adjusted last_ordered_3pc and shifted
    # the watermarks correspondingly
    assert node.viewNo == view_no
    assert replica.isPrimary
    assert replica.lastPrePrepareSeqNo == seq_no
    assert replica.last_ordered_3pc == (view_no, seq_no)
    assert replica.h == seq_no
    assert replica.H == seq_no + LOG_SIZE

    # Verify also that the stored last sent PrePrepare key has not been erased
    assert LAST_SENT_PRE_PREPARE in node.nodeStatusDB

    # Send a 3PC-batch and ensure that the replica orders it
    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               num_reqs=1, num_batches=1,
                               timeout=tconf.Max3PCBatchWait)

    seq_no = 8 if view_no == 0 else 9
    looper.run(
        eventually(lambda: assertExp(replica.last_ordered_3pc == (view_no, seq_no)),
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))
