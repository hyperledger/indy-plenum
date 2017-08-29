from plenum.common.messages.node_messages import Prepare
from plenum.test.batching_3pc.helper import make_node_syncing, fail_on_execute_batch_on_master
from plenum.test.helper import sendRandomRequests
from plenum.test.test_node import getNonPrimaryReplicas


def test_no_ordering_during_syncup(
        tconf, looper, txnPoolNodeSet, client, wallet1):
    non_primary_replica = getNonPrimaryReplicas(txnPoolNodeSet, instId=0)[0]

    # Put non-primary Node to syncing state once first Prepare is recieved
    make_node_syncing(
        non_primary_replica,
        Prepare)

    # Patch non-primary Node to fail if Order is executed
    fail_on_execute_batch_on_master(non_primary_replica.node)

    # Send requests. The non-primary Node should not fail since no ordering is
    # called while syncing
    sendRandomRequests(wallet1, client, tconf.Max3PCBatchSize)
    looper.runFor(5)
