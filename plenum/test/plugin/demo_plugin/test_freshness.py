import pytest

from plenum.common.constants import TXN_TYPE, DATA, DOMAIN_LEDGER_ID
from plenum.test.freshness.helper import get_multi_sig_values_for_all_nodes, \
    check_updated_bls_multi_sig_for_ledger, check_freshness_updated_for_ledger
from plenum.test.helper import freshness
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.constants import AUCTION_START
from plenum.test.plugin.demo_plugin.test_plugin_request_handling import successful_op
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


def test_update_bls_multi_sig_when_auction_ledger_orders(looper, tconf, txnPoolNodeSet,
                                                         sdk_pool_handle,
                                                         sdk_wallet_steward):
    # 1. Update auction ledger
    send_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)

    # 2. Wait for first freshness update
    looper.run(eventually(
        check_freshness_updated_for_ledger, txnPoolNodeSet, AUCTION_LEDGER_ID,
        timeout=3 * FRESHNESS_TIMEOUT)
    )
    domain_multi_sigs_after_first_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                              DOMAIN_LEDGER_ID)

    # 3. Update auction ledger
    looper.runFor(1)  # delay update
    send_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)

    # 4. Wait for the second freshness update.
    #  It's expected for Domain ledger, but not for the Auction
    auction_bls_multi_sigs_after_txns_sent = get_multi_sig_values_for_all_nodes(txnPoolNodeSet, AUCTION_LEDGER_ID)
    looper.run(eventually(check_updated_bls_multi_sig_for_ledger,
                          txnPoolNodeSet, DOMAIN_LEDGER_ID,
                          domain_multi_sigs_after_first_update,
                          FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT + 5
                          ))
    auction_bls_multi_sigs_after_domain_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                    AUCTION_LEDGER_ID)
    assert auction_bls_multi_sigs_after_txns_sent == auction_bls_multi_sigs_after_domain_update

    # 5. Third freshness update should update the acution ledger,
    # while it's too early to update the domain ledger yet
    domain_bls_multi_sigs_after_domain_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                   DOMAIN_LEDGER_ID)
    looper.run(eventually(check_updated_bls_multi_sig_for_ledger,
                          txnPoolNodeSet, AUCTION_LEDGER_ID,
                          auction_bls_multi_sigs_after_domain_update,
                          FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT + 5
                          ))
    assert domain_bls_multi_sigs_after_domain_update == get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                           DOMAIN_LEDGER_ID)


def send_auction_txn(looper,
                     sdk_pool_handle, sdk_wallet_stewards):
    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'abc'}
    }
    successful_op(looper, op, sdk_wallet_stewards, sdk_pool_handle)
