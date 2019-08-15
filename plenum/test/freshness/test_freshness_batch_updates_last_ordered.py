import pytest

from plenum.common.messages.node_messages import PrePrepare

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.freshness.helper import get_all_multi_sig_values_for_all_nodes, \
    check_updated_bls_multi_sig_for_all_ledgers, check_freshness_updated_for_all
from plenum.test.helper import freshness, sdk_send_random_and_check, primary_disconnection_time
from plenum.test.spy_helpers import getSpecificDiscardedMsg
from plenum.test.view_change.helper import restart_node

from stp_core.loop.eventually import eventually

# 10 secs, so node could restart
FRESHNESS_TIMEOUT = 10


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT), primary_disconnection_time(tconf, 100):
        yield tconf


def test_freshness_batch_updates_last_ordered(looper, txnPoolNodeSet, sdk_pool_handle,
                                              sdk_wallet_steward, tconf, tdir, allPluginsPath):
    assert txnPoolNodeSet[0].master_replica.isPrimary

    looper.run(eventually(
        check_freshness_updated_for_all, txnPoolNodeSet,
        timeout=FRESHNESS_TIMEOUT * 2)
    )
    bls_multi_sigs_after_first_update = get_all_multi_sig_values_for_all_nodes(txnPoolNodeSet)
    looper.run(eventually(check_updated_bls_multi_sig_for_all_ledgers,
                          txnPoolNodeSet, bls_multi_sigs_after_first_update, FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT * 2
                          ))

    restart_node(looper, txnPoolNodeSet, txnPoolNodeSet[0], tconf, tdir, allPluginsPath)

    # no view change happened
    assert txnPoolNodeSet[0].master_replica.isPrimary

    assert txnPoolNodeSet[0].db_manager.get_txn_root_hash(DOMAIN_LEDGER_ID) == \
           txnPoolNodeSet[1].db_manager.get_txn_root_hash(DOMAIN_LEDGER_ID)
    # node caught up till actual last_ordered_3pc
    assert txnPoolNodeSet[0].master_replica.last_ordered_3pc == \
           txnPoolNodeSet[1].master_replica.last_ordered_3pc

    old_discard = len(getSpecificDiscardedMsg(txnPoolNodeSet[1], PrePrepare))

    # correct ordering
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 1)

    # domain ledger equeal
    assert txnPoolNodeSet[0].db_manager.get_txn_root_hash(DOMAIN_LEDGER_ID) == \
           txnPoolNodeSet[1].db_manager.get_txn_root_hash(DOMAIN_LEDGER_ID)

    # no discard happened
    assert len(getSpecificDiscardedMsg(txnPoolNodeSet[1], PrePrepare)) == old_discard


def test_freshness_batch_updates_last_ordered_non_primary(looper, txnPoolNodeSet, sdk_pool_handle,
                                                          sdk_wallet_steward, tconf, tdir, allPluginsPath):
    looper.run(eventually(
        check_freshness_updated_for_all, txnPoolNodeSet,
        timeout=FRESHNESS_TIMEOUT * 2)
    )
    bls_multi_sigs_after_first_update = get_all_multi_sig_values_for_all_nodes(txnPoolNodeSet)
    looper.run(eventually(check_updated_bls_multi_sig_for_all_ledgers,
                          txnPoolNodeSet, bls_multi_sigs_after_first_update, FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT * 2
                          ))

    restart_node(looper, txnPoolNodeSet, txnPoolNodeSet[1], tconf, tdir, allPluginsPath)

    assert txnPoolNodeSet[0].db_manager.get_txn_root_hash(DOMAIN_LEDGER_ID) == \
           txnPoolNodeSet[1].db_manager.get_txn_root_hash(DOMAIN_LEDGER_ID)
    # node caught up till actual last_ordered_3pc
    assert txnPoolNodeSet[0].master_replica.last_ordered_3pc == \
           txnPoolNodeSet[1].master_replica.last_ordered_3pc

    # correct ordering
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 1)

    # domain ledger equeal
    assert txnPoolNodeSet[0].db_manager.get_txn_root_hash(DOMAIN_LEDGER_ID) == \
           txnPoolNodeSet[1].db_manager.get_txn_root_hash(DOMAIN_LEDGER_ID)
