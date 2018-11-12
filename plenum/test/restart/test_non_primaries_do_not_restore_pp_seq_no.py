from plenum.test import waits
from plenum.test.helper import sdk_send_batches_of_random, assertExp
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, getPrimaryReplica
from plenum.test.view_change.helper import ensure_view_change, \
    start_stopped_node
from stp_core.loop.eventually import eventually

nodeCount = 7


def test_non_primaries_do_not_restore_pp_seq_no(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
        tconf, tdir, allPluginsPath):

    for _ in range(2):
        ensure_view_change(looper, txnPoolNodeSet)
        ensureElectionsDone(looper, txnPoolNodeSet)

    inst_1_primary_node = getPrimaryReplica(txnPoolNodeSet, instId=1).node

    assert inst_1_primary_node.viewNo == 2

    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               num_reqs=5, num_batches=5,
                               timeout=tconf.Max3PCBatchWait)

    for inst_id in inst_1_primary_node.replicas.keys():
        looper.run(
            eventually(lambda: assertExp(inst_1_primary_node.replicas[inst_id].last_ordered_3pc == (2, 5)),
                       retryWait=1, timeout=waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))))

    assert inst_1_primary_node.viewNo == 2
    assert inst_1_primary_node.replicas[0].lastPrePrepareSeqNo == 5
    assert inst_1_primary_node.replicas[2].lastPrePrepareSeqNo == 5

    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            inst_1_primary_node.name,
                                            stopNode=True)
    looper.removeProdable(inst_1_primary_node)
    txnPoolNodeSet.remove(inst_1_primary_node)

    restarted_node = start_stopped_node(inst_1_primary_node, looper,
                                             tconf, tdir, allPluginsPath)
    txnPoolNodeSet.append(restarted_node)

    ensureElectionsDone(looper, txnPoolNodeSet,
                        customTimeout=waits.expectedPoolCatchupTime(len(txnPoolNodeSet)) +
                                      waits.expectedPoolElectionTimeout(len(txnPoolNodeSet)))

    assert restarted_node.viewNo == 2
    assert restarted_node.replicas[1].isPrimary
    assert restarted_node.replicas[0].lastPrePrepareSeqNo == 0
    assert restarted_node.replicas[2].lastPrePrepareSeqNo == 0
