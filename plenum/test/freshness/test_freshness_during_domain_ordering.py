import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID
from plenum.test.freshness.helper import check_update_bls_multi_sig_during_ordering
from plenum.test.helper import freshness, sdk_send_random_and_check

FRESHNESS_TIMEOUT = 10


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_update_bls_multi_sig_during_domain_ordering(looper, tconf, txnPoolNodeSet,
                                                     sdk_pool_handle,
                                                     sdk_wallet_stewards):

    def send_txn():
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_stewards[3],
                                  1)

    check_update_bls_multi_sig_during_ordering(looper, txnPoolNodeSet,
                                               send_txn,
                                               FRESHNESS_TIMEOUT,
                                               DOMAIN_LEDGER_ID, POOL_LEDGER_ID)
