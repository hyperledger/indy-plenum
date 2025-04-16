import pytest

from plenum.test.replica_removing.helper import do_test_replica_removing_with_backup_degraded, replica_removing


@pytest.fixture(scope="module")
def tconf(tconf):
    with replica_removing(tconf, acc_monitor_enabled=True, replica_remove_stratgey="quorum"):
        yield tconf


def test_replica_removing_with_backup_degraded(looper,
                                               txnPoolNodeSet,
                                               vdr_pool_handle,
                                               vdr_wallet_client,
                                               vdr_wallet_steward,
                                               tconf,
                                               tdir,
                                               allPluginsPath):
    do_test_replica_removing_with_backup_degraded(looper,
                                                  txnPoolNodeSet,
                                                  vdr_pool_handle,
                                                  vdr_wallet_client,
                                                  tconf)
