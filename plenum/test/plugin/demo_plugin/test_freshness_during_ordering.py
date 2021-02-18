import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.freshness.helper import check_update_bls_multi_sig_during_ordering
from plenum.test.helper import freshness
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.helper import send_auction_txn

FRESHNESS_TIMEOUT = 10


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_update_bls_multi_sig_when_auction_ledger_orders(looper, tconf, txnPoolNodeSet,
                                                         sdk_pool_handle,
                                                         sdk_wallet_steward):
    #  Update auction ledger so that its state root is different from config ledger
    for node in txnPoolNodeSet:
        node.states[AUCTION_LEDGER_ID].set(b'some_key', b'some_value')

    def send_txn():
        send_auction_txn(looper,
                         sdk_pool_handle, sdk_wallet_steward)

    check_update_bls_multi_sig_during_ordering(looper, txnPoolNodeSet,
                                               send_txn,
                                               FRESHNESS_TIMEOUT,
                                               AUCTION_LEDGER_ID, DOMAIN_LEDGER_ID)
