from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data

from plenum.test.helper import sdk_send_random_requests, sdk_send_random_and_check, sdk_send_batches_of_random_and_check

from plenum.test.primary_selection.helper import getPrimaryNodesIdxs

from plenum.test import waits
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.restart.helper import get_group, restart_nodes

nodeCount = 7
batches_count = 3
def get_primary_replicas(txnPoolNodeSet):
    idxs = getPrimaryNodesIdxs(txnPoolNodeSet)
    for idx in

def test_persist_last_pp(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    primary_replicas = [txnPoolNodeSet[0].replicas._replicas[0],
                        txnPoolNodeSet[1].replicas._replicas[1],
                        txnPoolNodeSet[2].replicas._replicas[2]]
    assert primary_replicas[0].isPrimary and primary_replicas[0].isMaster
    assert primary_replicas[1].isPrimary
    assert primary_replicas[2].isPrimary

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, batches_count, batches_count)
    # Check that we've persisted last send pre-prepare on every primary node
    assert primary_replicas[0].last_ordered_3pc == \
           primary_replicas[0].lastPrePrepare
    assert primary_replicas[1].last_ordered_3pc == \
           primary_replicas[1].lastPrePrepare
    assert primary_replicas[2].last_ordered_3pc == \
           primary_replicas[2].lastPrePrepare


def test_clear_persist_last_pp_after_restart(looper, txnPoolNodeSet, tconf, tdir,
                                             sdk_pool_handle, sdk_wallet_client, allPluginsPath):
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, batches_count,
                                         batches_count)

    # Restart master primary to make a view_change
    restart_nodes(looper, txnPoolNodeSet, [txnPoolNodeSet[0]], tconf, tdir, allPluginsPath,
                  after_restart_timeout=tconf.ToleratePrimaryDisconnection + 1)
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, batches_count,
                                         batches_count)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    assert first_replica_of_beta.isPrimary

    # Check that we've cleared persisted value

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, batches_count,
                                         batches_count)

    # Check that we've persisted right value
    assert second_replica_of_beta.last_ordered_3pc == second_replica_of_beta.lastPrePrepare == (1, 3)

    # Turn off sofe backup primary and make a view_change

    # Restart the pool
    restart_nodes(looper, txnPoolNodeSet, [txnPoolNodeSet[1]], tconf, tdir, allPluginsPath)
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, batches_count,
                                         batches_count)
    assert second_replica_of_beta.last_ordered_3pc[1] == second_replica_of_beta.lastPrePrepareSeqNo == 0

    # Restart
    assert second_replica_of_beta.last_ordered_3pc[1] == second_replica_of_beta.lastPrePrepareSeqNo == 3
    restart_nodes(looper, txnPoolNodeSet, [txnPoolNodeSet[1]], tconf, tdir, allPluginsPath)
    assert second_replica_of_beta.last_ordered_3pc[1] == second_replica_of_beta.lastPrePrepareSeqNo == 3

    # Check that we've applied last send pre-prepare after restart
    # Pool is working
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, batches_count,
                                         batches_count)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
