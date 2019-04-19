import pytest

from plenum.test import waits
from plenum.test.delayers import cDelay, chk_delay, icDelay, vcd_delay
from plenum.test.helper import sdk_send_random_and_check, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules

CHK_FREQ = 2
LOG_SIZE = 2 * CHK_FREQ

Max3PCBatchSize = 1


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


def test_watermarks_after_view_change(tdir, tconf,
                                      looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_client):
    """
    Delay commit, checkpoint, InstanceChange and ViewChangeDone messages for lagging_node.
    Start ViewChange.
    Check that ViewChange finished.
    Reset delays.
    Check that lagging_node can order transactions and has same data with other nodes.
    """
    lagging_node = txnPoolNodeSet[-1]
    lagging_node.master_replica.config.LOG_SIZE = LOG_SIZE
    start_view_no = lagging_node.viewNo
    with delay_rules(lagging_node.nodeIbStasher, cDelay(), chk_delay(), icDelay(), vcd_delay()):
        for n in txnPoolNodeSet:
            n.view_changer.on_master_degradation()
        waitForViewChange(looper,
                          txnPoolNodeSet[:-1],
                          expectedViewNo=start_view_no + 1,
                          customTimeout=waits.expectedPoolViewChangeStartedTimeout(len(txnPoolNodeSet)))
        ensure_all_nodes_have_same_data(looper, txnPoolNodeSet[:-1])
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_client, 6)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
