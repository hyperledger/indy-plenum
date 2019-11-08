import pytest
import sys

from plenum.server.replica_validator_enums import STASH_WATERMARKS
from plenum.test.delayers import nv_delay
from plenum.test.helper import sdk_send_random_and_check, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import add_new_node, ensure_view_change

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


def test_set_H_greater_then_last_ppseqno(looper,
                                         txnPoolNodeSet,
                                         sdk_pool_handle,
                                         sdk_wallet_steward,
                                         tdir,
                                         tconf,
                                         allPluginsPath):
    start_view_no = txnPoolNodeSet[0].viewNo
    # send LOG_SIZE requests and check, that all watermarks on all replicas is not changed
    # and now is (0, LOG_SIZE)
    """Send random requests for moving watermarks"""
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, LOG_SIZE)
    # check, that all of node set up watermark greater, then default and
    # ppSeqNo with number LOG_SIZE + 1 will be out from default watermark
    assert txnPoolNodeSet[0].replicas[1].last_ordered_3pc[1] == LOG_SIZE
    for n in txnPoolNodeSet:
        for r in n.replicas._replicas.values():
            assert r.h >= LOG_SIZE
            assert r.H >= LOG_SIZE + LOG_SIZE
    """Adding new node, for scheduling propagate primary procedure"""
    new_node = add_new_node(looper, txnPoolNodeSet, sdk_pool_handle,
                            sdk_wallet_steward, tdir, tconf, allPluginsPath)
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=start_view_no + 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet,
                                    exclude_from_check=['check_last_ordered_3pc_backup'])
    """Check, that backup replicas set watermark as (0, maxInt)"""
    # Check, replica.h is set from last_ordered_3PC and replica.H is set to maxsize
    for r in new_node.replicas.values():
        if r.isMaster:
            assert r.h == r.last_ordered_3pc[1]
            assert r.H == r.last_ordered_3pc[1] + LOG_SIZE
        else:
            assert r.h == 0
            assert r.H == sys.maxsize
    """Send requests and check. that backup replicas does not stashing it by outside watermarks reason"""
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 1)
    # check, that there is no any stashed "outside watermark" messages.
    for r in new_node.replicas.values():
        assert r.stasher.stash_size(STASH_WATERMARKS) == 0

    """Force view change and check, that all backup replicas will reset watermarks"""
    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)
    for r in new_node.replicas.values():
        if not r.isMaster:
            assert r.h == 0
            assert r.H == LOG_SIZE
