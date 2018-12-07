from plenum.test import waits
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.test_node import checkNodesConnected

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

    assert len(master_replica.checkpoints) == 0

    assert len(master_replica.stashedRecvdCheckpoints) == 0

    assert master_replica.h == 2
    assert master_replica.H == 17

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    for replica in new_node.replicas.values():
        assert len(replica.checkpoints) == 1

        assert len(replica.stashedRecvdCheckpoints) == 0

        assert replica.h == 2
        assert replica.H == 17

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 6)
    stabilization_timeout = \
        waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.runFor(stabilization_timeout)

    for replica in new_node.replicas.values():
        assert len(replica.checkpoints) == 2
        keys_iter = iter(replica.checkpoints)

        assert next(keys_iter) == (3, 5)
        assert replica.checkpoints[3, 5].seqNo == 5
        assert replica.checkpoints[3, 5].digest is None
        assert replica.checkpoints[3, 5].isStable is False

        assert next(keys_iter) == (6, 10)
        assert replica.checkpoints[6, 10].seqNo == 9
        assert replica.checkpoints[6, 10].digest is None
        assert replica.checkpoints[6, 10].isStable is False

        assert len(replica.stashedRecvdCheckpoints) == 1
        assert 0 in replica.stashedRecvdCheckpoints
        assert len(replica.stashedRecvdCheckpoints[0]) == 1
        assert (1, 5) in replica.stashedRecvdCheckpoints[0]
        assert len(replica.stashedRecvdCheckpoints[0][(1, 5)]) == 4

        assert replica.h == 2
        assert replica.H == 17

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)
    looper.runFor(stabilization_timeout)

    for replica in new_node.replicas.values():
        assert len(replica.checkpoints) == 1
        keys_iter = iter(replica.checkpoints)

        assert next(keys_iter) == (6, 10)
        assert replica.checkpoints[6, 10].seqNo == 10
        assert replica.checkpoints[6, 10].digest is not None
        assert replica.checkpoints[6, 10].isStable is True

        assert len(replica.stashedRecvdCheckpoints) == 0

        assert replica.h == 10
        assert replica.H == 25
