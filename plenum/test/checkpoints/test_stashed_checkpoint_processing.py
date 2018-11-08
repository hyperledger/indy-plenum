from plenum.common.constants import COMMIT, CHECKPOINT
from plenum.test import waits
from plenum.test.delayers import cDelay, chk_delay
from plenum.test.helper import sdk_send_random_and_check
from stp_core.loop.eventually import eventually

CHK_FREQ = 5

nodeCount = 5


def test_stashed_checkpoint_processing(chkFreqPatched, looper, txnPoolNodeSet,
                                       sdk_wallet_client, sdk_pool_handle):
    """
    One node in a pool of 5 nodes lags to order the last 3PC-batch in a
    checkpoint. By the moment when it eventually orders the 3PC-batch it has
    already received and stashed Checkpoint message from two node, so it
    processes these stashed messages on completing the checkpoint. After this
    it receives Checkpoint messages from two other nodes, processes them and
    stabilizes the checkpoint.
    """
    epsilon = txnPoolNodeSet[-1]

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 4)

    epsilon.nodeIbStasher.delay(cDelay())
    epsilon.nodeIbStasher.delay(chk_delay(sender_filter='Gamma'))
    epsilon.nodeIbStasher.delay(chk_delay(sender_filter='Delta'))

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    stabilization_timeout = \
        waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.runFor(stabilization_timeout)

    for inst_id, replica in epsilon.replicas.items():
        assert len(replica.checkpoints) == 1
        assert (1, 5) in replica.checkpoints
        assert replica.checkpoints[(1, 5)].seqNo == 4
        assert replica.checkpoints[(1, 5)].digest is None
        assert replica.checkpoints[(1, 5)].isStable is False

        assert len(replica.stashedRecvdCheckpoints) == 1
        assert 0 in replica.stashedRecvdCheckpoints
        assert len(replica.stashedRecvdCheckpoints[0]) == 1
        assert (1, 5) in replica.stashedRecvdCheckpoints[0]
        assert len(replica.stashedRecvdCheckpoints[0][(1, 5)]) == 2

    epsilon.nodeIbStasher.reset_delays_and_process_delayeds(COMMIT)

    def check():
        for inst_id, replica in epsilon.replicas.items():
            assert len(replica.checkpoints) == 1
            assert (1, 5) in replica.checkpoints
            assert replica.checkpoints[(1, 5)].seqNo == 5
            assert replica.checkpoints[(1, 5)].digest is not None
            assert replica.checkpoints[(1, 5)].isStable is False

            assert len(replica.stashedRecvdCheckpoints) == 0

    looper.run(eventually(check, timeout=waits.expectedOrderingTime(
        len(txnPoolNodeSet))))

    epsilon.nodeIbStasher.reset_delays_and_process_delayeds(CHECKPOINT)

    stabilization_timeout = \
        waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.runFor(stabilization_timeout)

    for inst_id, replica in epsilon.replicas.items():
        assert len(replica.checkpoints) == 1
        assert (1, 5) in replica.checkpoints
        assert replica.checkpoints[(1, 5)].seqNo == 5
        assert replica.checkpoints[(1, 5)].digest is not None
        assert replica.checkpoints[(1, 5)].isStable is True

        assert len(replica.stashedRecvdCheckpoints) == 0
