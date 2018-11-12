from plenum.test import waits
from plenum.test.helper import sdk_send_batches_of_random, assertExp
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, getPrimaryReplica
from plenum.test.view_change.helper import ensure_view_change, \
    start_stopped_node
from stp_core.loop.eventually import eventually

nodeCount = 7


def test_backup_replica_does_not_restore_pp_seq_no_if_not_primary_anymore(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
        tconf, tdir, allPluginsPath):

    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    inst_2_primary_node = getPrimaryReplica(txnPoolNodeSet, instId=2).node

    assert inst_2_primary_node.viewNo == 1

    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               num_reqs=5, num_batches=5,
                               timeout=tconf.Max3PCBatchWait)

    looper.run(
        eventually(lambda: assertExp(inst_2_primary_node.replicas[2].last_ordered_3pc == (1, 5)),
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

    assert inst_2_primary_node.viewNo == 1
    assert inst_2_primary_node.replicas[2].lastPrePrepareSeqNo == 5

    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            inst_2_primary_node.name,
                                            stopNode=True)
    looper.removeProdable(inst_2_primary_node)
    txnPoolNodeSet.remove(inst_2_primary_node)

    ensure_view_change(looper, txnPoolNodeSet)
    def chk():
        for node in txnPoolNodeSet:
            for replica in node.replicas.values():
                assert replica.hasPrimary
    # We can't use ensureElectionsDone below to ensure that elections are done
    # because the primary of instance 1 is off. So we use chk instead.
    looper.run(eventually(chk, retryWait=1, timeout=waits.expectedPoolElectionTimeout(nodeCount)))

    restarted_node = start_stopped_node(inst_2_primary_node, looper, tconf, tdir, allPluginsPath)
    txnPoolNodeSet.append(restarted_node)

    ensureElectionsDone(looper, txnPoolNodeSet,
                        customTimeout=waits.expectedPoolCatchupTime(nodeCount) +
                                      waits.expectedPoolElectionTimeout(nodeCount))

    assert restarted_node.viewNo == 2

    assert not restarted_node.replicas[2].isPrimary
    assert restarted_node.replicas[2].lastPrePrepareSeqNo == 0

    # In addition verify that the replica 1 which is now the primary in its
    # instance does not use the persisted value of a foreign instance for
    # lastPrePrepareSeqNo restoration
    assert restarted_node.replicas[1].isPrimary
    assert restarted_node.replicas[2].lastPrePrepareSeqNo == 0
