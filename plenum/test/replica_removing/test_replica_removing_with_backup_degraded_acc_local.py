import pytest

from plenum.test.replica_removing.helper import do_test_replica_removing_with_backup_degraded, replica_removing


@pytest.fixture(scope="module")
def tconf(tconf):
    with replica_removing(tconf, acc_monitor_enabled=False, replica_remove_stratgey="local"):
        yield tconf


def test_replica_removing_with_backup_degraded(looper,
                                               txnPoolNodeSet,
                                               sdk_pool_handle,
                                               sdk_wallet_client,
                                               sdk_wallet_steward,
                                               tconf,
                                               tdir,
                                               allPluginsPath):
    do_test_replica_removing_with_backup_degraded(looper,
                                                  txnPoolNodeSet,
                                                  sdk_pool_handle,
                                                  sdk_wallet_client,
                                                  tconf)
