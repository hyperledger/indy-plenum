from plenum.test import waits
from plenum.test.checkpoints.helper import check_stable_checkpoint, check_num_received_checkpoints, \
    check_num_unstable_checkpoints, check_last_checkpoint
from plenum.test.helper import sdk_send_random_and_check, assertExp
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.test_node import checkNodesConnected
from stp_core.loop.eventually import eventually

CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ


def test_second_checkpoint_after_catchup_can_be_stabilized(
        chkFreqPatched, looper, txnPoolNodeSet, sdk_wallet_steward,
        sdk_wallet_client, sdk_pool_handle, tdir, tconf,
        allPluginsPath):
    _, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        'EpsilonSteward', 'Epsilon', tdir, tconf,
        allPluginsPath=allPluginsPath)
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
    # Epsilon did not participate in ordering of the batch with EpsilonSteward
    # NYM transaction and the batch with Epsilon NODE transaction.
    # Epsilon got these transactions via catch-up.

    master_replica = new_node.replicas._master_replica

    check_stable_checkpoint(master_replica, 0)
    check_num_received_checkpoints(master_replica, 0)

    assert master_replica.h == 2
    assert master_replica.H == 17

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    for replica in new_node.replicas.values():
        assert replica.h == 2
        assert replica.H == 17

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 6)
    stabilization_timeout = \
        waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.runFor(stabilization_timeout)

    for replica in new_node.replicas.values():
        check_stable_checkpoint(replica, 5)
        check_num_unstable_checkpoints(replica, 0)

        # nothing is stashed since it's ordered during catch-up
        check_num_received_checkpoints(replica, 0)

        assert replica.h == 5
        assert replica.H == 20

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)
    looper.runFor(stabilization_timeout)

    for replica in new_node.replicas.values():
        check_stable_checkpoint(replica, 10)
        check_num_unstable_checkpoints(replica, 0)

        # nothing is stashed since it's ordered during catch-up
        check_num_received_checkpoints(replica, 0)

        assert replica.h == 10
        assert replica.H == 25
