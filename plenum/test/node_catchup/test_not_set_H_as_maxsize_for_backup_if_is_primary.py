import pytest

from plenum.test.delayers import delay_3pc
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import start_stopped_node, ensure_view_change
from stp_core.loop.eventually import eventually


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


def test_set_H_as_maxsize_for_backup_if_is_primary(looper,
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

    # Stop Node
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, primary_on_backup, stopNode=True)
    txnPoolNodeSet.remove(primary_on_backup)
    looper.removeProdable(primary_on_backup)

    # Start stopped Node
    primary_on_backup = start_stopped_node(primary_on_backup, looper, tconf, tdir, allPluginsPath)

    # Delay 3PC messages so that when restarted node does not have them ordered
    with delay_rules(primary_on_backup.nodeIbStasher, delay_3pc()):
        txnPoolNodeSet.append(primary_on_backup)

        ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=tconf.NEW_VIEW_TIMEOUT)

        sdk_send_random_and_check(looper,
                                  txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_steward,
                                  LOG_SIZE)

        # Check restored state
        assert primary_on_backup.replicas._replicas[1].isPrimary
        assert primary_on_backup.replicas._replicas[1].h == 1
        assert primary_on_backup.replicas._replicas[1].H == 1 + LOG_SIZE

    def chk():
        assert primary_on_backup.replicas._replicas[1].h == LOG_SIZE
        assert primary_on_backup.replicas._replicas[1].H == LOG_SIZE + LOG_SIZE

    # Check caught-up state
    looper.run(eventually(chk, retryWait=.2, timeout=tconf.NEW_VIEW_TIMEOUT))
