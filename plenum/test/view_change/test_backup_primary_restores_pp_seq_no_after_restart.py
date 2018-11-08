from plenum.test import waits
from plenum.test.helper import sdk_send_batches_of_random, assertExp
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, getPrimaryReplica
from plenum.test.view_change.helper import ensure_view_change, \
    start_stopped_node
from stp_core.loop.eventually import eventually

backup_inst_id = 1


def test_backup_primary_restores_pp_seq_no_after_restart(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
        tconf, tdir, allPluginsPath):

    for _ in range(2):
        ensure_view_change(looper, txnPoolNodeSet)
        ensureElectionsDone(looper, txnPoolNodeSet)

    backup_primary_replica = getPrimaryReplica(txnPoolNodeSet, instId=backup_inst_id)
    backup_primary_node = backup_primary_replica.node

    assert backup_primary_replica.viewNo == 2

    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               num_reqs=5, num_batches=5,
                               timeout=tconf.Max3PCBatchWait)

    looper.run(
        eventually(lambda: assertExp(backup_primary_replica.last_ordered_3pc == (2, 5)),
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))))

    assert backup_primary_replica.viewNo == 2
    assert backup_primary_replica.lastPrePrepareSeqNo == 5

    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            backup_primary_node.name,
                                            stopNode=True)
    looper.removeProdable(backup_primary_node)
    txnPoolNodeSet.remove(backup_primary_node)

    backup_primary_node = start_stopped_node(backup_primary_node, looper,
                                             tconf, tdir, allPluginsPath)
    txnPoolNodeSet.append(backup_primary_node)

    ensureElectionsDone(looper, txnPoolNodeSet,
                        customTimeout=waits.expectedPoolCatchupTime(len(txnPoolNodeSet)) +
                                      waits.expectedPoolElectionTimeout(len(txnPoolNodeSet)))
    backup_primary_replica = backup_primary_node.replicas[backup_inst_id]

    assert backup_primary_replica.isPrimary
    assert backup_primary_replica.viewNo == 2
    assert backup_primary_replica.lastPrePrepareSeqNo == 5
    assert backup_primary_replica.last_ordered_3pc == (2, 5)
