from logging import getLogger

from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.server.replica import Replica
from plenum.test import waits

from plenum.test.helper import send_reqs_batches_and_get_suff_replies
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    checkNodeDataForInequality, get_number_of_completed_catchups
from plenum.test.test_node import getNonPrimaryReplicas

logger = getLogger()

TestRunningTimeLimitSec = 200


CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ


def test_lag_size_for_catchup(
        looper, chkFreqPatched, reqs_for_checkpoint, txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_client):
    """
    Verifies that if the stored own checkpoints have aligned bounds then
    the master replica lag which makes the node perform catch-up is
    Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1 quorumed stashed received
    checkpoints.
    """
    slow_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1].node
    other_nodes = [n for n in txnPoolNodeSet if n != slow_node]

    # The master replica of the slow node stops to receive 3PC-messages
    slow_node.master_replica.threePhaseRouter.extend(
        (
            (PrePrepare, lambda *x, **y: None),
            (Prepare, lambda *x, **y: None),
            (Commit, lambda *x, **y: None),
        )
    )

    completed_catchups_before_reqs = get_number_of_completed_catchups(slow_node)

    # Send requests for the slow node's master replica to get
    # Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP quorumed stashed checkpoints
    # from others
    send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP *
                                           reqs_for_checkpoint)

    # Give time for the slow node to catch up if it is going to do it
    looper.runFor(waits.expectedPoolConsistencyProof(len(txnPoolNodeSet)) +
                  waits.expectedPoolCatchupTime(len(txnPoolNodeSet)))

    checkNodeDataForInequality(slow_node, *other_nodes)

    # Verify that the slow node has not caught up
    assert get_number_of_completed_catchups(slow_node) == completed_catchups_before_reqs

    # Send more requests for the slow node's master replica to reach
    # Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1 quorumed stashed
    # checkpoints from others
    send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           reqs_for_checkpoint)

    waitNodeDataEquality(looper, slow_node, *other_nodes)

    # Verify that the slow node has caught up
    assert get_number_of_completed_catchups(slow_node) > completed_catchups_before_reqs
