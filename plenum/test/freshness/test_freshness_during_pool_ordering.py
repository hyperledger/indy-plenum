import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID
from plenum.test.bls.helper import sdk_change_bls_key
from plenum.test.freshness.helper import check_update_bls_multi_sig_during_ordering
from plenum.test.helper import freshness

FRESHNESS_TIMEOUT = 10


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_update_bls_multi_sig_during_pool_ordering(looper, tconf, txnPoolNodeSet,
                                                   sdk_pool_handle,
                                                   sdk_wallet_stewards):
    def send_txn():
        sdk_change_bls_key(looper, txnPoolNodeSet,
                           txnPoolNodeSet[3],
                           sdk_pool_handle,
                           sdk_wallet_stewards[3],
                           check_functional=False)

    check_update_bls_multi_sig_during_ordering(looper, txnPoolNodeSet,
                                               send_txn,
                                               FRESHNESS_TIMEOUT,
                                               POOL_LEDGER_ID, DOMAIN_LEDGER_ID)
