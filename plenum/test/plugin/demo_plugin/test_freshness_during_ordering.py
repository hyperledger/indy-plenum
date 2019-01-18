import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.freshness.helper import check_freshness_updated_for_ledger, get_multi_sig_values_for_all_nodes, \
    check_updated_bls_multi_sig_for_ledger, check_freshness_updated_for_all
from plenum.test.helper import freshness
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.helper import send_auction_txn
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 10


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_update_bls_multi_sig_when_auction_ledger_orders(looper, tconf, txnPoolNodeSet,
                                                         sdk_pool_handle,
                                                         sdk_wallet_steward):
    # 1. Update auction ledger so that its state root is different from config ledger
    # Delay update so that next freshness update of auction is not too close to domain freshness update
    looper.runFor(3)
    send_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)
    auction_multi_sigs_after_txns_sent = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                            AUCTION_LEDGER_ID)

    # 2. Wait for freshness update of other ledgers, make sure that auction signatures are not updated
    looper.run(eventually(
        check_freshness_updated_for_all, txnPoolNodeSet,
        timeout=FRESHNESS_TIMEOUT + 5)
    )
    domain_multi_sigs_after_first_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                              DOMAIN_LEDGER_ID)
    assert auction_multi_sigs_after_txns_sent == get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                    AUCTION_LEDGER_ID)

    # 3. Update auction ledger, make sure domain signatures are not updated
    send_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)
    auction_multi_sigs_after_txns_sent = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                            AUCTION_LEDGER_ID)
    assert domain_multi_sigs_after_first_update == get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                      DOMAIN_LEDGER_ID)

    # 4. Wait for the second freshness update of domain ledger
    # Again it's expected for domain ledger, but not for the auction
    looper.run(eventually(check_updated_bls_multi_sig_for_ledger,
                          txnPoolNodeSet, DOMAIN_LEDGER_ID,
                          domain_multi_sigs_after_first_update,
                          FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT + 5
                          ))
    domain_bls_multi_sigs_after_domain_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                   DOMAIN_LEDGER_ID)
    auction_multi_sigs_after_domain_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                AUCTION_LEDGER_ID)
    assert auction_multi_sigs_after_txns_sent == auction_multi_sigs_after_domain_update

    # 5. Third freshness update should update the auction ledger,
    # while it's too early to update the domain ledger yet
    looper.run(eventually(check_updated_bls_multi_sig_for_ledger,
                          txnPoolNodeSet, AUCTION_LEDGER_ID,
                          auction_multi_sigs_after_domain_update,
                          FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT + 5
                          ))
    assert auction_multi_sigs_after_domain_update != get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                        AUCTION_LEDGER_ID)
    assert domain_bls_multi_sigs_after_domain_update == get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                           DOMAIN_LEDGER_ID)
