import sys

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, COMMIT
from plenum.server.replica import Replica
from plenum.test import waits
from plenum.test.checkpoints.helper import check_num_quorumed_received_checkpoints, check_num_unstable_checkpoints
from plenum.test.delayers import cDelay, chk_delay, msg_rep_delay
from plenum.test.helper import sdk_send_random_requests, assertExp, sdk_send_random_and_check, assert_eq, get_pp_seq_no, \
    check_last_ordered_3pc_backup
from stp_core.loop.eventually import eventually

nodeCount = 4

CHK_FREQ = 6
LOG_SIZE = 3 * CHK_FREQ
first_run = True


@pytest.fixture(scope="module")
def tconf(tconf):
    old = tconf.Max3PCBatchesInFlight
    # This test requires lots of batches in flight (actually 8) in order to function properly,
    # so we allow any number to simplify things
    tconf.Max3PCBatchesInFlight = None
    yield tconf
    tconf.Max3PCBatchesInFlight = old


def test_backup_replica_resumes_ordering_on_lag_in_checkpoints(
        looper, chkFreqPatched, reqs_for_checkpoint,
        one_replica_and_others_in_backup_instance,
        sdk_pool_handle, sdk_wallet_client, view_change_done, txnPoolNodeSet):
    """
    Verifies resumption of ordering 3PC-batches on a backup replica
    on detection of a lag in checkpoints
    """
    slow_replica, other_replicas = one_replica_and_others_in_backup_instance
    view_no = slow_replica.viewNo
    batches_count = slow_replica.last_ordered_3pc[1]

    # Send a request and ensure that the replica orders the batch for it
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)
    batches_count += 1
    low_watermark = slow_replica.h

    looper.run(
        eventually(lambda: assert_eq(slow_replica.last_ordered_3pc, (view_no, batches_count)),
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

    # Recover reception of Commits
    slow_replica.node.nodeIbStasher.drop_delayeds()
    slow_replica.node.nodeIbStasher.resetDelays()

    # Send requests but in a quantity insufficient
    # for catch-up number of checkpoints
    reqs_until_checkpoints = reqs_for_checkpoint - other_replicas[0].last_ordered_3pc[1]
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client,
                             Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP *
                             reqs_until_checkpoints)
    looper.runFor(waits.expectedTransactionExecutionTime(nodeCount))

    # Ensure that the replica has not ordered any batches
    # after the very first one
    assert slow_replica.last_ordered_3pc == (view_no, batches_count)

    # Ensure that the watermarks have not been shifted since the view start
    assert slow_replica.h == low_watermark
    assert slow_replica.H == low_watermark + LOG_SIZE

    # Ensure that the collections related to requests, batches and
    # own checkpoints are not empty.
    # (Note that a primary replica removes requests from requestQueues
    # when creating a batch with them.)
    if slow_replica.isPrimary:
        assert slow_replica._ordering_service.sent_preprepares
    else:
        assert slow_replica._ordering_service.requestQueues[DOMAIN_LEDGER_ID]
        assert slow_replica._ordering_service.prePrepares
    assert slow_replica._ordering_service.prepares
    assert slow_replica._ordering_service.commits
    assert slow_replica._ordering_service.batches

    check_num_unstable_checkpoints(slow_replica, 0)
    check_num_quorumed_received_checkpoints(slow_replica, 1)

    # Send more requests to reach catch-up number of checkpoints
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, reqs_for_checkpoint)
    batches_count += 1
    batches_count += reqs_until_checkpoints
    batches_count += reqs_for_checkpoint
    # Ensure that the replica has adjusted last_ordered_3pc to the end
    # of the last checkpoint
    looper.run(
        eventually(lambda *args: assertExp(slow_replica.last_ordered_3pc == \
                        (view_no, batches_count)),
                   slow_replica,
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

    # Ensure that the watermarks have been shifted so that the lower watermark
    # has the same value as last_ordered_3pc
    assert slow_replica.h == low_watermark + (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) * CHK_FREQ
    assert slow_replica.H == low_watermark + (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) * CHK_FREQ + LOG_SIZE

    # Ensure that the collections related to requests, batches and
    # own checkpoints have been cleared
    assert not slow_replica._ordering_service.requestQueues[DOMAIN_LEDGER_ID]
    assert not slow_replica._ordering_service.sent_preprepares
    assert not slow_replica._ordering_service.prePrepares
    assert not slow_replica._ordering_service.prepares
    assert not slow_replica._ordering_service.commits
    assert not slow_replica._ordering_service.batches

    check_num_unstable_checkpoints(slow_replica, 0)
    check_num_quorumed_received_checkpoints(slow_replica, 0)

    # Send a request and ensure that the replica orders the batch for it
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)
    batches_count += 1

    looper.run(
        eventually(lambda *args: assertExp(slow_replica.last_ordered_3pc ==
                                     (view_no, batches_count)),
                   slow_replica,
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))
    slow_replica._checkpointer._received_checkpoints.clear()
    batches_count = get_pp_seq_no(txnPoolNodeSet)


def test_backup_replica_resumes_ordering_on_lag_if_checkpoints_belate(
        looper, chkFreqPatched, reqs_for_checkpoint,
        one_replica_and_others_in_backup_instance,
        sdk_pool_handle, sdk_wallet_client, view_change_done, txnPoolNodeSet):
    """
    Verifies resumption of ordering 3PC-batches on a backup replica
    on detection of a lag in checkpoints in case it is detected after
    some batch in the next checkpoint has already been committed but cannot
    be ordered out of turn
    """
    def check_last_ordered(replica, lo):
        assert replica.last_ordered_3pc == lo

    slow_replica, other_replicas = one_replica_and_others_in_backup_instance
    view_no = slow_replica.viewNo
    check_last_ordered_3pc_backup(slow_replica.node, other_replicas[0].node)
    batches_count = slow_replica.last_ordered_3pc[1]
    low_watermark = slow_replica.h

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
    looper.run(
        eventually(lambda *args: assertExp(slow_replica.last_ordered_3pc == (view_no, batches_count)),
                   slow_replica,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

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
    # Send a request that starts a new checkpoint
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)
    looper.runFor(waits.expectedTransactionExecutionTime(nodeCount))

    # Ensure that the replica has not ordered any batches
    # after the very first one
    assert slow_replica.last_ordered_3pc == (view_no, batches_count)

    # Ensure that the watermarks have not been shifted since the view start
    assert slow_replica.h == low_watermark
    assert slow_replica.H == low_watermark + LOG_SIZE

    # Ensure that there are some quorumed stashed checkpoints
    check_num_quorumed_received_checkpoints(slow_replica, 1)

    # Receive belated Checkpoints
    slow_replica.node.nodeIbStasher.reset_delays_and_process_delayeds()

    batches_count += 1
    batches_count += reqs_until_checkpoints
    batches_count += reqs_for_checkpoint
    batches_count += 1
    # Ensure that the replica has ordered the batch for the last sent request
    looper.run(
        eventually(check_last_ordered, slow_replica, (view_no, batches_count),
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

    # Ensure that the watermarks have been shifted so that the lower watermark
    # now equals to the end of the last stable checkpoint in the instance
    assert slow_replica.h == low_watermark + (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) * CHK_FREQ
    assert slow_replica.H == low_watermark + (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) * CHK_FREQ + LOG_SIZE

    # Ensure that now there are no quorumed stashed checkpoints
    check_num_quorumed_received_checkpoints(slow_replica, 0)

    # Send a request and ensure that the replica orders the batch for it
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)
    batches_count += 1

    looper.run(
        eventually(lambda: assertExp(slow_replica.last_ordered_3pc ==
                                     (view_no, batches_count)),
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))
