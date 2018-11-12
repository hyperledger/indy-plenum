from plenum.test import waits
from plenum.test.helper import sdk_send_batches_of_random, assertExp, \
    sdk_send_random_and_check
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, getPrimaryReplica, \
    getRequiredInstances
from plenum.test.view_change.helper import ensure_view_change, \
    start_stopped_node
from stp_core.loop.eventually import eventually

nodeCount = 4

backup_inst_id = 1


def test_backup_primary_does_not_restore_pp_seq_no_if_view_is_not_same(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
        tconf, tdir, allPluginsPath):

    for _ in range(6):
        ensure_view_change(looper, txnPoolNodeSet)
        ensureElectionsDone(looper, txnPoolNodeSet)

    backup_primary_replica = getPrimaryReplica(txnPoolNodeSet, instId=backup_inst_id)
    backup_primary_node = backup_primary_replica.node
    other_nodes = set(txnPoolNodeSet) - {backup_primary_node}

    assert backup_primary_node.viewNo == 6

    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               num_reqs=5, num_batches=5,
                               timeout=tconf.Max3PCBatchWait)

    looper.run(
        eventually(lambda: assertExp(backup_primary_replica.last_ordered_3pc == (6, 5)),
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

    assert backup_primary_node.viewNo == 6
    assert backup_primary_replica.lastPrePrepareSeqNo == 5

    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            backup_primary_node.name,
                                            stopNode=True)
    looper.removeProdable(backup_primary_node)

    # Restart the rest of the nodes
    current_node_set = set(other_nodes)
    for node in other_nodes:
        disconnect_node_and_ensure_disconnected(looper,
                                                current_node_set,
                                                node.name,
                                                timeout=nodeCount,
                                                stopNode=True)
        looper.removeProdable(node)
        current_node_set.remove(node)
    for node in other_nodes:
        current_node_set.add(start_stopped_node(node, looper, tconf, tdir, allPluginsPath))
    ensureElectionsDone(looper, current_node_set,
                        instances_list=range(getRequiredInstances(nodeCount)),
                        customTimeout=waits.expectedPoolCatchupTime(nodeCount) +
                                      waits.expectedPoolElectionTimeout(nodeCount))

    # Ensure a number of view changes to bring the pool into a view with
    # another number than before the restart but with the same primaries
    ensure_view_change(looper, current_node_set)
    ensureElectionsDone(looper, current_node_set,
                        instances_list=range(getRequiredInstances(nodeCount)))

    ensure_view_change(looper, current_node_set)
    def chk():
        for node in current_node_set:
            for replica in node.replicas.values():
                assert replica.hasPrimary
    # We can't use ensureElectionsDone below to ensure that elections are done
    # because the primary of instance 1 is off. So we use chk instead.
    looper.run(eventually(chk, retryWait=1, timeout=waits.expectedPoolElectionTimeout(nodeCount)))

    # Send a request to have last_ordered_3pc not equal to (0, 0) on the master instance
    sdk_send_random_and_check(looper, current_node_set, sdk_pool_handle, sdk_wallet_client, 1)

    restarted_node = start_stopped_node(backup_primary_node, looper, tconf, tdir, allPluginsPath)
    current_node_set.add(restarted_node)

    ensureElectionsDone(looper, current_node_set,
                        customTimeout=waits.expectedPoolCatchupTime(nodeCount) +
                                      waits.expectedPoolElectionTimeout(nodeCount))

    assert restarted_node.viewNo == 2
    assert restarted_node.replicas[backup_inst_id].isPrimary
    assert restarted_node.replicas[backup_inst_id].lastPrePrepareSeqNo == 0
