import sys
import pytest

from plenum.common.constants import COMMIT
from plenum.server.replica import Replica
from plenum.server.replica_validator_enums import STASH_WATERMARKS
from plenum.test import waits
from plenum.test.checkpoints.helper import check_num_quorumed_received_checkpoints
from plenum.test.delayers import cDelay, chk_delay, msg_rep_delay
from plenum.test.helper import sdk_send_random_requests, assertExp, incoming_3pc_msgs_count, get_pp_seq_no
from stp_core.loop.eventually import eventually

nodeCount = 4

CHK_FREQ = 6

# LOG_SIZE in checkpoints corresponds to the catch-up lag in checkpoints
LOG_SIZE = 2 * CHK_FREQ

first_run = True


@pytest.fixture(scope="module")
def tconf(tconf):
    old = tconf.Max3PCBatchesInFlight
    # This test requires lots of batches in flight (actually 9) in order to function properly,
    # so we allow any number to simplify things
    tconf.Max3PCBatchesInFlight = None
    yield tconf
    tconf.Max3PCBatchesInFlight = old


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
    batches_count = other_replicas[0].last_ordered_3pc[1]

    # Send a request and ensure that the replica orders the batch for it
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)
    batches_count += 1

    looper.run(
        eventually(lambda *args: assertExp(slow_replica.last_ordered_3pc == (view_no, batches_count)),
                   slow_replica,
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

    # Don't receive Commits from two replicas
    slow_replica.node.nodeIbStasher.delay(
        cDelay(instId=1, sender_filter=other_replicas[0].node.name))
    slow_replica.node.nodeIbStasher.delay(
        cDelay(instId=1, sender_filter=other_replicas[1].node.name))
    slow_replica.node.nodeIbStasher.delay(
        msg_rep_delay(types_to_delay=[COMMIT])
    )

    # Send a request for which the replica will not be able to order the batch
    # due to an insufficient count of Commits
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)
    looper.runFor(waits.expectedTransactionExecutionTime(nodeCount))

    # Receive further Commits from now on
    slow_replica.node.nodeIbStasher.drop_delayeds()
    slow_replica.node.nodeIbStasher.resetDelays()

    # Send requests but in a quantity insufficient
    # for catch-up number of checkpoints
    reqs_until_checkpoints = reqs_for_checkpoint - other_replicas[0].last_ordered_3pc[1]
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client,
                             Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP *
                             reqs_until_checkpoints)
    looper.runFor(waits.expectedTransactionExecutionTime(nodeCount))

    # Don't receive Checkpoints
    slow_replica.node.nodeIbStasher.delay(chk_delay(instId=1))

    # Send more requests to reach catch-up number of checkpoints
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client,
                             reqs_for_checkpoint)
    looper.runFor(waits.expectedTransactionExecutionTime(nodeCount))

    # Ensure that there are no 3PC-messages stashed
    # as laying outside of the watermarks
    assert slow_replica.stasher.stash_size(STASH_WATERMARKS) == 0

    # Send a request for which the batch will be outside of the watermarks
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)
    looper.runFor(waits.expectedTransactionExecutionTime(nodeCount))

    # Ensure that the replica has not ordered any batches
    # after the very first one
    assert slow_replica.last_ordered_3pc == (view_no, batches_count)

    # Ensure that the watermarks have not been shifted since the view start
    assert slow_replica.h == 0
    assert slow_replica.H == LOG_SIZE

    # Ensure that there are some quorumed stashed checkpoints
    check_num_quorumed_received_checkpoints(slow_replica, 1)

    # Ensure that now there are 3PC-messages stashed
    # as laying outside of the watermarks
    assert slow_replica.stasher.stash_size(STASH_WATERMARKS) == incoming_3pc_msgs_count(len(txnPoolNodeSet))

    # Receive belated Checkpoints
    slow_replica.node.nodeIbStasher.reset_delays_and_process_delayeds()
    batches_count = other_replicas[0].last_ordered_3pc[1]

    # Ensure that the replica has ordered the batch for the last sent request
    looper.run(
        eventually(lambda *args: assertExp(slow_replica.last_ordered_3pc ==
                                     (view_no, batches_count)),
                   slow_replica,
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

    # Ensure that the watermarks have been shifted so that the lower watermark
    # now equals to the end of the last stable checkpoint in the instance
    assert slow_replica.h == (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) * CHK_FREQ
    assert slow_replica.H == (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) * CHK_FREQ + LOG_SIZE

    # Ensure that now there are no quorumed stashed checkpoints
    check_num_quorumed_received_checkpoints(slow_replica, 0)

    # Ensure that now there are no 3PC-messages stashed
    # as laying outside of the watermarks
    assert slow_replica.stasher.stash_size(STASH_WATERMARKS) == 0

    # Send a request and ensure that the replica orders the batch for it
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)
    batches_count += 1

    looper.run(
        eventually(lambda *args: assertExp(slow_replica.last_ordered_3pc ==
                                           (view_no, batches_count)),
                   slow_replica,
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))
