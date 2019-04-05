from logging import getLogger

import sys

from plenum.common.messages.node_messages import PrePrepare
from plenum.test.helper import sdk_send_batches_of_random_and_check
from plenum.test.test_node import getNonPrimaryReplicas

logger = getLogger()

inst_id = 1

CHK_FREQ = 5
LOG_SIZE = 4 * CHK_FREQ


def test_watermarks_restored_after_stable(
        looper,
        chkFreqPatched,
        txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_client):
    """
    A backup replica doesn't participate in consensus, and hence doesn't update watermarks.
    Then if it gets a quorum of stashed checkpoints (in fact Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1 checkpoints
    where we have a quorum of n-f-1 for each),
    then it updates the watermarks,
    and starts to participate in consensus.
    """

    # 1. patch backup replica on a non-primary node so that it doesn't participate
    # in consensus, so that watermarks are not updated on it.
    broken_replica, non_broken_replica = break_backup_replica(txnPoolNodeSet)

    # 2. send the number of requests which is less than Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1
    # quorumed checkpoints,
    # but sufficient for one watermark change (on a non-broken replica).
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                                         num_reqs=1 * 9, num_batches=9)
    assert broken_replica.last_ordered_3pc == (0, 0)
    assert broken_replica.h == 0
    assert broken_replica.H == sys.maxsize
    assert non_broken_replica.last_ordered_3pc == (0, 9)
    assert non_broken_replica.h == 5
    assert non_broken_replica.H == 25

    # 3. send requests to reach Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1
    # quorumed checkpoints.
    # The broken replica should adjust last_ordered_3pc and shift watermarks.
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                                         num_reqs=1, num_batches=1)
    assert broken_replica.last_ordered_3pc == (0, 10)
    assert broken_replica.h == 10
    assert broken_replica.H == 30
    assert non_broken_replica.last_ordered_3pc == (0, 10)
    assert non_broken_replica.h == 10
    assert non_broken_replica.H == 30

    # 4. Repair broken replica and make sure that it participates in consensus
    # (after watermarks were corrected).
    repair_broken_replica(broken_replica)
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                                         num_reqs=7, num_batches=7)
    assert broken_replica.last_ordered_3pc == (0, 17)
    assert broken_replica.h == 15
    assert broken_replica.H == 35
    assert non_broken_replica.last_ordered_3pc == (0, 17)
    assert non_broken_replica.h == 15
    assert non_broken_replica.H == 35


def break_backup_replica(txnPoolNodeSet):
    node = getNonPrimaryReplicas(txnPoolNodeSet, inst_id)[-1].node
    broken_replica = node.replicas[inst_id]
    non_broken_replica = node.replicas[0]

    def fakeProcessPrePrepare(pre_prepare):
        logger.warning(
            "{} is broken. 'processPrePrepare' does nothing".format(broken_replica.name))

    broken_replica.threePhaseRouter.extend(
        (
            (PrePrepare, fakeProcessPrePrepare),
        )
    )

    return broken_replica, non_broken_replica


def repair_broken_replica(replica):
    replica.threePhaseRouter.extend(
        (
            (PrePrepare, replica.processPrePrepare),
        )
    )
    return replica
