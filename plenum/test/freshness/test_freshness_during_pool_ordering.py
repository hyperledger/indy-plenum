import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID
from plenum.test.freshness.helper import check_update_bls_multi_sig_during_ordering
from plenum.test.helper import freshness

FRESHNESS_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_update_bls_multi_sig_during_pool_ordering(looper, tconf, txnPoolNodeSet,
                                                     sdk_pool_handle,
                                                     sdk_wallet_stewards):

    check_update_bls_multi_sig_during_ordering(looper, txnPoolNodeSet,
                                               sdk_pool_handle, sdk_wallet_stewards,
                                               FRESHNESS_TIMEOUT,
                                               POOL_LEDGER_ID, DOMAIN_LEDGER_ID)
