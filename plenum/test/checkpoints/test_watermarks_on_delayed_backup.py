from logging import getLogger

from plenum.common.messages.node_messages import PrePrepare
from plenum.test.helper import sdk_send_batches_of_random_and_check
from plenum.test.test_node import getNonPrimaryReplicas

logger = getLogger()

TestRunningTimeLimitSec = 200

inst_id = 1

CHK_FREQ = 5
LOG_SIZE = 2 * CHK_FREQ


def test_watermarks_restored_after_stable(
        looper,
        chkFreqPatched,
        txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_client):
    """
    A backup replica doesn't participate in consensus, and hence doesn't update watermarks.
    Then if gets a quorum of stable checkpoints, updates the watermarks,
    and starts to participate oin consensus.
    """
    # 1. patch backup replica on a non-primary node so that it doesn't participate
    # in consensus, so that watermarks are not updated on it.
    broken_replica, non_broken_replica = break_backup_replica(txnPoolNodeSet)

    # 2. send the number of requests which is less than a stable checkpoint,
    # but sufficient for one watermark change (on a non-broken replica).
    num_batches = 9
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                                         num_reqs=1 * num_batches, num_batches=num_batches)
    assert broken_replica.last_ordered_3pc == (0, 0)
    assert broken_replica.h == 0
    assert broken_replica.H == 10
    assert non_broken_replica.last_ordered_3pc == (0, 9)
    assert non_broken_replica.h == 5
    assert non_broken_replica.H == 15

    # 3. send requests to reach a stable checkpoint.
    # The broken replica should correct watermarks.
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                                         num_reqs=1, num_batches=1)
    assert broken_replica.last_ordered_3pc == (0, 0)
    assert broken_replica.h == 10
    assert broken_replica.H == 20
    assert non_broken_replica.last_ordered_3pc == (0, 10)
    assert non_broken_replica.h == 10
    assert non_broken_replica.H == 20

    # 4. Repair broken replica amd make sure that it p[articipates in consensus
    # (after watermarks were corrected).
    repair_broken_replica(broken_replica)
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                                         num_reqs=7, num_batches=7)
    assert broken_replica.last_ordered_3pc == (0, 17)
    assert broken_replica.h == 15
    assert broken_replica.H == 25
    assert non_broken_replica.last_ordered_3pc == (0, 17)
    assert non_broken_replica.h == 15
    assert non_broken_replica.H == 25


def break_backup_replica(txnPoolNodeSet):
    node = getNonPrimaryReplicas(txnPoolNodeSet, inst_id)[-1].node
    broken_replica = node.replicas[inst_id]
    non_broken_replica = node.replicas[0]

    def fakeProcessPrePrepare(pre_prepare, sender):
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
