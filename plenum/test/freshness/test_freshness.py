import pytest

from common.serializers.serialization import state_roots_serializer
from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID
from plenum.test.bls.helper import sdk_change_bls_key
from plenum.test.helper import assertExp, freshness, sdk_send_random_and_check
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_update_bls_multi_sig_by_timeout(looper, tconf, txnPoolNodeSet):
    # 1. Wait for the first freshness update
    looper.run(eventually(
        lambda: assertExp(
            all(
                bls_multi_sig is not None
                for bls_multi_sig in get_all_multi_sig_values_for_all_nodes(txnPoolNodeSet)
            )
        ),
        timeout=FRESHNESS_TIMEOUT + 5
    ))

    # 2. Wait for the second freshness update
    bls_multi_sigs_after_first_update = get_all_multi_sig_values_for_all_nodes(txnPoolNodeSet)
    looper.run(eventually(check_updated_bls_multi_sig_for_all_ledgers,
                          txnPoolNodeSet, bls_multi_sigs_after_first_update,
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
        lambda: assertExp(
            all(
                bls_multi_sig is not None
                for bls_multi_sig in get_all_multi_sig_values_for_all_nodes(txnPoolNodeSet)
            )
        ),
        timeout=FRESHNESS_TIMEOUT + 5
    ))
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
                          timeout=FRESHNESS_TIMEOUT + 5
                          ))
    assert refreshed_bls_multi_sigs_after_refreshed_update == get_multi_sig_values_for_all_nodes(txnPoolNodeSet,
                                                                                                 refreshed_ledger_id)


def check_updated_bls_multi_sig_for_all_ledgers(nodes, previous_multi_sigs):
    multi_sigs = get_all_multi_sig_values_for_all_nodes(nodes)
    for i, (bls_multi_sig, previous_bls_multi_sig) in enumerate(zip(multi_sigs, previous_multi_sigs)):
        check_updated_bls_multisig_values(i, bls_multi_sig, previous_bls_multi_sig)


def check_updated_bls_multi_sig_for_ledger(nodes, ledger_id, previous_multi_sigs):
    multi_sigs = get_multi_sig_values_for_all_nodes(nodes, ledger_id)
    for i, (bls_multi_sig, previous_bls_multi_sig) in enumerate(zip(multi_sigs, previous_multi_sigs)):
        check_updated_bls_multisig_values(i, bls_multi_sig, previous_bls_multi_sig)


def check_updated_bls_multisig_values(i, new_value, old_value):
    assert new_value is not None, \
        "{}: value is None {}".format(i, new_value)
    assert new_value.timestamp - old_value.timestamp >= FRESHNESS_TIMEOUT, \
        "{}: freshness delta is {}".format(i, new_value.timestamp - old_value.timestamp)
    assert new_value.state_root_hash == old_value.state_root_hash, \
        "{}: new state root {}, old state root {}".format(i, new_value.state_root_hash, old_value.state_root_hash)
    assert new_value.txn_root_hash == old_value.txn_root_hash, \
        "{}: new txn root {}, txn state root {}".format(i, new_value.txn_root_hash, old_value.txn_root_hash)


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


def get_all_multi_sig_values_for_all_nodes(txnPoolNodeSet):
    result = []
    for node in txnPoolNodeSet:
        for state in node.states.values():
            bls_multi_sig = node.bls_bft.bls_store.get(
                state_roots_serializer.serialize(bytes(state.committedHeadHash))
            )
            result.append(bls_multi_sig.value if bls_multi_sig else None)
    return result


def get_multi_sig_values_for_all_nodes(txnPoolNodeSet, ledger_id):
    result = []
    for node in txnPoolNodeSet:
        bls_multi_sig = node.bls_bft.bls_store.get(
            state_roots_serializer.serialize(bytes(node.states[ledger_id].committedHeadHash))
        )
        result.append(bls_multi_sig.value if bls_multi_sig else None)
    return result
