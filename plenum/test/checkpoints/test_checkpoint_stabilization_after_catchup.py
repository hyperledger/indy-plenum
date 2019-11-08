from plenum.test.delayers import cDelay

from plenum.test import waits
from plenum.test.checkpoints.helper import check_stable_checkpoint, check_num_received_checkpoints, \
    check_num_unstable_checkpoints
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.stasher import delay_rules_without_processing
CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ


def test_second_checkpoint_after_catchup_can_be_stabilized(
        chkFreqPatched, looper, txnPoolNodeSet, sdk_wallet_steward,
        sdk_wallet_client, sdk_pool_handle, tdir, tconf,
        allPluginsPath):
    lagging_node = txnPoolNodeSet[-1]
    with delay_rules_without_processing(lagging_node.nodeIbStasher, cDelay()):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_client, tconf.Max3PCBatchSize * CHK_FREQ * 2)
    waitNodeDataEquality(looper, lagging_node, *txnPoolNodeSet[:-1])
    # Epsilon got lost transactions via catch-up.
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 2)

    master_replica = lagging_node.master_replica

    check_stable_checkpoint(master_replica, 10)
    check_num_received_checkpoints(master_replica, 0)

    assert master_replica.h == 10
    assert master_replica.H == 25

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    for replica in lagging_node.replicas.values():
        assert replica.h == 10
        assert replica.H == 25

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 6)
    stabilization_timeout = \
        waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.runFor(stabilization_timeout)

    for replica in lagging_node.replicas.values():
        check_stable_checkpoint(replica, 15)
        check_num_unstable_checkpoints(replica, 0)

        # nothing is stashed since it's ordered during catch-up
        check_num_received_checkpoints(replica, 0)

        assert replica.h == 15
        assert replica.H == 30

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)
    looper.runFor(stabilization_timeout)

    for replica in lagging_node.replicas.values():
        check_stable_checkpoint(replica, 20)
        check_num_unstable_checkpoints(replica, 0)

        # nothing is stashed since it's ordered during catch-up
        check_num_received_checkpoints(replica, 0)

        assert replica.h == 20
        assert replica.H == 35
