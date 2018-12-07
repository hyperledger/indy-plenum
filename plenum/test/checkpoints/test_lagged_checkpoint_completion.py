from plenum.test import waits
from plenum.test.delayers import cDelay
from plenum.test.helper import sdk_send_random_and_check
from stp_core.loop.eventually import eventually

CHK_FREQ = 5


def test_lagged_checkpoint_completion(chkFreqPatched, looper, txnPoolNodeSet,
                                      sdk_wallet_client, sdk_pool_handle):
    """
    One node in a pool lags to order the last 3PC-batch in a checkpoint so that
    when it eventually orders this 3PC-batch and thus completes the checkpoint
    it has already received and stashed the corresponding checkpoint messages
    from all the other nodes. The test verifies that the node successfully
    processes the stashed checkpoint messages and stabilizes the checkpoint.
    """
    slow_node = txnPoolNodeSet[-1]

    # All the nodes in the pool normally orders all the 3PC-batches in a
    # checkpoint except the last 3PC-batch. The last 3PC-batch in the
    # checkpoint is ordered by all the nodes except one slow node because this
    # node lags to receive Commits.
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 4)

    slow_node.nodeIbStasher.delay(cDelay())

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    # All the other nodes complete the checkpoint and send Checkpoint messages
    # to others. The slow node receives and stashes these messages because it
    # has not completed the checkpoint.
    def check():
        for replica in slow_node.replicas.values():
            assert len(replica.checkpoints) == 1
            assert (1, 5) in replica.checkpoints
            assert replica.checkpoints[(1, 5)].seqNo == 4
            assert replica.checkpoints[(1, 5)].digest is None
            assert replica.checkpoints[(1, 5)].isStable is False

            assert len(replica.stashedRecvdCheckpoints) == 1
            assert 0 in replica.stashedRecvdCheckpoints
            assert len(replica.stashedRecvdCheckpoints[0]) == 1
            assert (1, 5) in replica.stashedRecvdCheckpoints[0]
            assert len(replica.stashedRecvdCheckpoints[0][(1, 5)]) == \
                len(txnPoolNodeSet) - 1

    stabilization_timeout = \
        waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(eventually(check, timeout=stabilization_timeout))

    # Eventually the slow node receives Commits, orders the last 3PC-batch in
    # the checkpoint and thus completes it, processes the stashed checkpoint
    # messages and stabilizes the checkpoint.
    slow_node.nodeIbStasher.reset_delays_and_process_delayeds()

    looper.runFor(waits.expectedOrderingTime(len(txnPoolNodeSet)))

    for replica in slow_node.replicas.values():
        assert len(replica.checkpoints) == 1
        assert (1, 5) in replica.checkpoints
        assert replica.checkpoints[(1, 5)].seqNo == 5
        assert replica.checkpoints[(1, 5)].digest is not None
        assert replica.checkpoints[(1, 5)].isStable is True

        assert len(replica.stashedRecvdCheckpoints) == 0
