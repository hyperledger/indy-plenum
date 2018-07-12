import pytest

from plenum.server.replica import Replica
from plenum.test.helper import send_reqs_batches_and_get_suff_replies
from plenum.test.node_catchup.helper import waitNodeDataInequality, waitNodeDataEquality
from plenum.test.node_catchup.test_node_catchup_after_checkpoints import repair_broken_node, \
    get_number_of_completed_catchups
from plenum.test.checkpoints.conftest import tconf, chkFreqPatched, \
    reqs_for_checkpoint
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change

Max3PCBatchSize = 1
CHK_FREQ = 5
LOG_SIZE = CHK_FREQ * 3


@pytest.fixture(scope='module')
def tconf(tconf):
    old_max_3pc_batch_size = tconf.Max3PCBatchSize
    old_log_size = tconf.LOG_SIZE
    old_chk_freq = tconf.CHK_FREQ
    tconf.Max3PCBatchSize = Max3PCBatchSize
    tconf.LOG_SIZE = LOG_SIZE
    tconf.CHK_FREQ = CHK_FREQ

    yield tconf
    tconf.Max3PCBatchSize = old_max_3pc_batch_size
    tconf.LOG_SIZE = old_log_size
    tconf.CHK_FREQ = old_chk_freq



def test_catchup_in_view_change_not_break_backup_watermarks(looper,
                                                            txnPoolNodeSet,
                                                            sdk_pool_handle,
                                                            sdk_wallet_client,
                                                            broken_node_and_others,
                                                            reqs_for_checkpoint):
    """
    A node misses 3pc messages and checkpoints during some period but later it
    stashes some amount of checkpoints and decides to catchup.
    """
    max_batch_size = Max3PCBatchSize
    broken_node, other_nodes = broken_node_and_others
    """Check, that watermarks are default"""
    for r in broken_node.replicas:
        if not r.isMaster:
            assert r.h == 0
            assert r.H == LOG_SIZE

    """Step 1: The node misses quite a lot of requests"""

    send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           reqs_for_checkpoint + max_batch_size)

    waitNodeDataInequality(looper, broken_node, *other_nodes)

    """Step 2: The node gets requests but cannot process them because of
       missed ones. But the nodes eventually stashes some amount checkpoints
       after that the node starts catch up"""
    completed_catchups_before = get_number_of_completed_catchups(broken_node)

    send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) *
                                           reqs_for_checkpoint - max_batch_size)
    """Check, that all replicas did not ordered txns"""
    for r in broken_node.replicas:
        assert r.last_ordered_3pc == (0, 0)

    """Step3: Forcing view_change and also catchup during view_change"""
    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    waitNodeDataEquality(looper, broken_node, *other_nodes)

    completed_catchups_after = get_number_of_completed_catchups(broken_node)
    assert completed_catchups_after > completed_catchups_before

    """Check, that after view_change with catchup all the replica set default watermark
       (not propagate_primary logic)"""

    for r in broken_node.replicas:
        if not r.isMaster:
            assert r.h == r.last_ordered_3pc[1]
            assert r.H == r.last_ordered_3pc[1] + LOG_SIZE





