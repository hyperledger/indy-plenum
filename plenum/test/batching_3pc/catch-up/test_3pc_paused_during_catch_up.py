from plenum.common.messages.node_messages import Prepare
from plenum.test.batching_3pc.helper import make_node_syncing, fail_on_execute_batch_on_master
from plenum.test.test_node import getNonPrimaryReplicas
from plenum.test.helper import sdk_send_random_requests


def test_sdk_no_ordering_during_syncup(tconf, looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    non_primary_replica = getNonPrimaryReplicas(txnPoolNodeSet, instId=0)[0]

    # Put non-primary Node to syncing state once first Prepare is recieved
    make_node_syncing(non_primary_replica, Prepare)

    # Patch non-primary Node to fail if Order is executed
    fail_on_execute_batch_on_master(non_primary_replica.node)

    # Send requests. The non-primary Node should not fail since no ordering is
    # called while syncing
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, tconf.Max3PCBatchSize)
    looper.runFor(5)
