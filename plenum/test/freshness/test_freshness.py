import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID
from plenum.test.bls.helper import sdk_change_bls_key
from plenum.test.freshness.helper import get_multi_sig_values_for_all_nodes, get_all_multi_sig_values_for_all_nodes, \
    check_updated_bls_multi_sig_for_all_ledgers, check_updated_bls_multi_sig_for_ledger, check_freshness_updated_for_all
from plenum.test.helper import freshness, sdk_send_random_and_check
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_update_bls_multi_sig_by_timeout(looper, tconf, txnPoolNodeSet):
    # 1. Wait for the first freshness update
    looper.run(eventually(
        check_freshness_updated_for_all, txnPoolNodeSet,
        timeout=FRESHNESS_TIMEOUT + 5)
    )

    # 2. Wait for the second freshness update
    bls_multi_sigs_after_first_update = get_all_multi_sig_values_for_all_nodes(txnPoolNodeSet)
    looper.run(eventually(check_updated_bls_multi_sig_for_all_ledgers,
                          txnPoolNodeSet, bls_multi_sigs_after_first_update, FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT + 5
                          ))


@pytest.mark.parametrize('ordered_ledger_id, refreshed_ledger_id', [
    (DOMAIN_LEDGER_ID, POOL_LEDGER_ID),
    (POOL_LEDGER_ID, DOMAIN_LEDGER_ID)
])
def test_update_bls_multi_sig_when_domain_orders(looper, tconf, txnPoolNodeSet,
                                                 sdk_pool_handle,
                                                 sdk_wallet_stewards,
                                                 ordered_ledger_id, refreshed_ledger_id):
    # 1. Wait for first freshness update
    looper.run(eventually(
        check_freshness_updated_for_all, txnPoolNodeSet,
        timeout=FRESHNESS_TIMEOUT + 5)
    )
    refreshed_bls_multi_sigs_after_first_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                     refreshed_ledger_id)

    # 2. order txns
    looper.runFor(1)  # delay update
    send_txn(ordered_ledger_id,
             looper, txnPoolNodeSet,
             sdk_pool_handle, sdk_wallet_stewards)

    # 3. Wait for the second freshness update.
    #  It's expected for the ledger we don't have any ordered requests only
    ordered_bls_multi_sigs_after_txns_sent = get_multi_sig_values_for_all_nodes(txnPoolNodeSet, ordered_ledger_id)
    looper.run(eventually(check_updated_bls_multi_sig_for_ledger,
                          txnPoolNodeSet, refreshed_ledger_id,
                          refreshed_bls_multi_sigs_after_first_update,
                          FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT + 5
                          ))
    ordered_bls_multi_sigs_after_refreshed_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                       ordered_ledger_id)
    assert ordered_bls_multi_sigs_after_txns_sent == ordered_bls_multi_sigs_after_refreshed_update

    # 4. Third freshness update should update the ledger where we had ordered requests,
    # while it's too early to update the second ledger yet
    refreshed_bls_multi_sigs_after_refreshed_update = get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                         refreshed_ledger_id)
    looper.run(eventually(check_updated_bls_multi_sig_for_ledger,
                          txnPoolNodeSet, ordered_ledger_id,
                          ordered_bls_multi_sigs_after_refreshed_update,
                          FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT + 5
                          ))
    assert refreshed_bls_multi_sigs_after_refreshed_update == get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                                 refreshed_ledger_id)


def send_txn(ordered_ledger_id,
             looper,
             txnPoolNodeSet,
             sdk_pool_handle, sdk_wallet_stewards):
    if ordered_ledger_id == DOMAIN_LEDGER_ID:
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_stewards[3],
                                  1)
    else:
        sdk_change_bls_key(looper, txnPoolNodeSet,
                           txnPoolNodeSet[3],
                           sdk_pool_handle,
                           sdk_wallet_stewards[3],
                           check_functional=False)
