from plenum.server.replica import Replica
from plenum.test import waits
from plenum.test.delayers import cDelay, chk_delay
from plenum.test.helper import sdk_send_random_requests, assertExp, incoming_3pc_msgs_count
from stp_core.loop.eventually import eventually

nodeCount = 4

CHK_FREQ = 5

# LOG_SIZE in checkpoints corresponds to the catch-up lag in checkpoints
LOG_SIZE = 2 * CHK_FREQ


def test_stashed_messages_processed_on_backup_replica_ordering_resumption(
        looper, chkFreqPatched, reqs_for_checkpoint,
        one_replica_and_others_in_backup_instance,
        sdk_pool_handle, sdk_wallet_client, view_change_done,
        txnPoolNodeSet):
    """
    Verifies resumption of ordering 3PC-batches on a backup replica
    on detection of a lag in checkpoints in case it is detected after
    some 3PC-messages related to the next checkpoint have already been stashed
    as laying outside of the watermarks.
    Please note that to verify this case the config is set up so that
    LOG_SIZE == (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) * CHK_FREQ
    """

    slow_replica, other_replicas = one_replica_and_others_in_backup_instance
    view_no = slow_replica.viewNo

    # Send a request and ensure that the replica orders the batch for it
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)

    looper.run(
        eventually(lambda *args: assertExp(slow_replica.last_ordered_3pc == (view_no, 2)),
                   slow_replica,
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

    # Don't receive Commits from two replicas
    slow_replica.node.nodeIbStasher.delay(
        cDelay(instId=1, sender_filter=other_replicas[0].node.name))
    slow_replica.node.nodeIbStasher.delay(
        cDelay(instId=1, sender_filter=other_replicas[1].node.name))

    # Send a request for which the replica will not be able to order the batch
    # due to an insufficient count of Commits
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)
    looper.runFor(waits.expectedTransactionExecutionTime(nodeCount))

    # Receive further Commits from now on
    slow_replica.node.nodeIbStasher.drop_delayeds()
    slow_replica.node.nodeIbStasher.resetDelays()

    # Send requests but in a quantity insufficient
    # for catch-up number of checkpoints
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client,
                             Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP *
                             reqs_for_checkpoint - 3)
    looper.runFor(waits.expectedTransactionExecutionTime(nodeCount))

    # Don't receive Checkpoints
    slow_replica.node.nodeIbStasher.delay(chk_delay(instId=1))

    # Send more requests to reach catch-up number of checkpoints
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client,
                             reqs_for_checkpoint)
    looper.runFor(waits.expectedTransactionExecutionTime(nodeCount))

    # Ensure that there are no 3PC-messages stashed
    # as laying outside of the watermarks
    assert slow_replica.stasher.num_stashed_watermarks == 0

    # Send a request for which the batch will be outside of the watermarks
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)
    looper.runFor(waits.expectedTransactionExecutionTime(nodeCount))

    # Ensure that the replica has not ordered any batches
    # after the very first one
    assert slow_replica.last_ordered_3pc == (view_no, 2)

    # Ensure that the watermarks have not been shifted since the view start
    assert slow_replica.h == 0
    assert slow_replica.H == LOG_SIZE

    # Ensure that there are some quorumed stashed checkpoints
    assert slow_replica.stashed_checkpoints_with_quorum()

    # Ensure that now there are 3PC-messages stashed
    # as laying outside of the watermarks
    assert slow_replica.stasher.num_stashed_watermarks == incoming_3pc_msgs_count(len(txnPoolNodeSet))

    # Receive belated Checkpoints
    slow_replica.node.nodeIbStasher.reset_delays_and_process_delayeds()

    # Ensure that the replica has ordered the batch for the last sent request
    looper.run(
        eventually(lambda *args: assertExp(slow_replica.last_ordered_3pc ==
                                     (view_no, (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) * CHK_FREQ + 1)),
                   slow_replica,
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

    # Ensure that the watermarks have been shifted so that the lower watermark
    # now equals to the end of the last stable checkpoint in the instance
    assert slow_replica.h == (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) * CHK_FREQ
    assert slow_replica.H == (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) * CHK_FREQ + LOG_SIZE

    # Ensure that now there are no quorumed stashed checkpoints
    assert not slow_replica.stashed_checkpoints_with_quorum()

    # Ensure that now there are no 3PC-messages stashed
    # as laying outside of the watermarks
    assert slow_replica.stasher.num_stashed_watermarks == 0

    # Send a request and ensure that the replica orders the batch for it
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)

    looper.run(
        eventually(lambda *args: assertExp(slow_replica.last_ordered_3pc ==
                                     (view_no, (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) * CHK_FREQ + 2)),
                   slow_replica,
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))
