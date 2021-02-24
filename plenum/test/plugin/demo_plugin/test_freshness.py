import pytest

from plenum.test.freshness.helper import get_multi_sig_values_for_all_nodes, \
    check_updated_bls_multi_sig_for_ledger, check_freshness_updated_for_ledger
from plenum.test.helper import freshness
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.helper import send_auction_txn
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_update_bls_multi_sig_for_auction_ledger_by_timeout(looper, tconf, txnPoolNodeSet,
                                                            sdk_pool_handle, sdk_wallet_steward):
    # 1. Update auction ledger
    send_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)

    # 2. Wait for the first freshness update
    looper.run(eventually(
        check_freshness_updated_for_ledger, txnPoolNodeSet, AUCTION_LEDGER_ID,
        timeout=3 * FRESHNESS_TIMEOUT)
    )

    # 3. Wait for the second freshness update
    bls_multi_sigs_after_first_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet, AUCTION_LEDGER_ID)
    looper.run(eventually(check_updated_bls_multi_sig_for_ledger,
                          txnPoolNodeSet, AUCTION_LEDGER_ID,
                          bls_multi_sigs_after_first_update,
                          FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT + 5
                          ))
