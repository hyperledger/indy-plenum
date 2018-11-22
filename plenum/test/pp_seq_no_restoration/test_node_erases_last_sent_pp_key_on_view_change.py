from plenum.common.constants import LAST_SENT_PRE_PREPARE
from plenum.test import waits
from plenum.test.helper import sdk_send_batches_of_random, assertExp
from plenum.test.test_node import ensureElectionsDone, getPrimaryReplica
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually

nodeCount = 4

backup_inst_id = 1


def test_node_erases_last_sent_pp_key_on_view_change(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, tconf):

    # Get a node with a backup primary replica
    replica = getPrimaryReplica(txnPoolNodeSet, instId=backup_inst_id)
    node = replica.node

    # Send some 3PC-batches and wait until the replica orders the 3PC-batches
    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               num_reqs=3, num_batches=3,
                               timeout=tconf.Max3PCBatchWait)

    looper.run(
        eventually(lambda: assertExp(replica.last_ordered_3pc == (0, 3)),
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

    # Ensure that there is a stored last sent PrePrepare key on the node
    assert LAST_SENT_PRE_PREPARE in node.nodeStatusDB

    # Make the pool perform view change
    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    # Verify that the node has erased the stored last sent PrePrepare key
    assert LAST_SENT_PRE_PREPARE not in node.nodeStatusDB

    # Send a 3PC-batch and ensure that the replica orders it
    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               num_reqs=1, num_batches=1,
                               timeout=tconf.Max3PCBatchWait)

    looper.run(
        eventually(lambda: assertExp(replica.last_ordered_3pc == (1, 1)),
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))
