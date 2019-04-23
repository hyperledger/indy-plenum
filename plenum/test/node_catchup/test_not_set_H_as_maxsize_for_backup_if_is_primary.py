import pytest

from plenum.test.helper import sdk_send_random_and_check
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import start_stopped_node, ensure_view_change

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



def test_not_set_H_as_maxsize_for_backup_if_is_primary(looper,
                                                       txnPoolNodeSet,
                                                       sdk_pool_handle,
                                                       sdk_wallet_steward,
                                                       tconf,
                                                       tdir,
                                                       allPluginsPath):
    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)
    primary_on_backup = txnPoolNodeSet[2]
    assert primary_on_backup.replicas._replicas[1].isPrimary
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            primary_on_backup,
                                            stopNode=True)
    looper.removeProdable(primary_on_backup)
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_steward,
                              LOG_SIZE)
    restarted_node = start_stopped_node(primary_on_backup,
                                        looper,
                                        tconf,
                                        tdir,
                                        allPluginsPath)
    txnPoolNodeSet[2] = restarted_node
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=tconf.VIEW_CHANGE_TIMEOUT)
    # Gamma catchup 1 txn
    assert restarted_node.replicas._replicas[1].isPrimary
    assert restarted_node.replicas._replicas[1].h == 1
    assert restarted_node.replicas._replicas[1].H == LOG_SIZE + 1