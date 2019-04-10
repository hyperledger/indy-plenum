import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID, CONFIG_LEDGER_ID
from plenum.test.audit_ledger.helper import check_audit_ledger_updated, check_audit_txn
from plenum.test.freshness.helper import check_freshness_updated_for_all, get_all_multi_sig_values_for_all_nodes, \
    check_updated_bls_multi_sig_for_all_ledgers
from plenum.test.helper import freshness
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 3


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_audit_ledger_updated_after_freshness_updated(looper, tconf, txnPoolNodeSet,
                                                      initial_domain_size, initial_pool_size, initial_config_size):
    # 1. Wait for the first freshness update
    looper.run(eventually(
        check_freshness_updated_for_all, txnPoolNodeSet,
        timeout=2 * FRESHNESS_TIMEOUT)
    )

    audit_size_initial = [node.auditLedger.size for node in txnPoolNodeSet]
    view_no = txnPoolNodeSet[0].master_replica.last_ordered_3pc[0]
    pp_seq_no = txnPoolNodeSet[0].master_replica.last_ordered_3pc[1]
    initial_seq_no = txnPoolNodeSet[0].auditLedger.size

    # 2. Wait for the second freshness update
    bls_multi_sigs_after_first_update = get_all_multi_sig_values_for_all_nodes(txnPoolNodeSet)
    looper.run(eventually(check_updated_bls_multi_sig_for_all_ledgers,
                          txnPoolNodeSet, bls_multi_sigs_after_first_update, FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT + 5
                          ))

    # 3. check that there is audit ledger txn created for each ledger updated as a freshness check
    check_audit_ledger_updated(audit_size_initial, txnPoolNodeSet,
                               audit_txns_added=3)
    for node in txnPoolNodeSet:
        check_audit_txn(txn=node.auditLedger.getBySeqNo(node.auditLedger.size - 2),
                        view_no=view_no, pp_seq_no=pp_seq_no + 1,
                        seq_no=initial_seq_no + 1, txn_time=node.master_replica.last_accepted_pre_prepare_time,
                        txn_roots={POOL_LEDGER_ID: node.getLedger(POOL_LEDGER_ID).tree.root_hash},
                        state_roots={POOL_LEDGER_ID: node.getState(POOL_LEDGER_ID).committedHeadHash},
                        pool_size=initial_pool_size, domain_size=initial_domain_size,
                        config_size=initial_config_size,
                        last_pool_seqno=None,
                        last_domain_seqno=2,
                        last_config_seqno=3,
                        primaries=pp_seq_no + 1 - 1)

        check_audit_txn(txn=node.auditLedger.getBySeqNo(node.auditLedger.size - 1),
                        view_no=view_no, pp_seq_no=pp_seq_no + 2,
                        seq_no=initial_seq_no + 2, txn_time=node.master_replica.last_accepted_pre_prepare_time,
                        txn_roots={DOMAIN_LEDGER_ID: node.getLedger(DOMAIN_LEDGER_ID).tree.root_hash},
                        state_roots={DOMAIN_LEDGER_ID: node.getState(DOMAIN_LEDGER_ID).committedHeadHash},
                        pool_size=initial_pool_size, domain_size=initial_domain_size,
                        config_size=initial_config_size,
                        last_pool_seqno=4,
                        last_domain_seqno=None,
                        last_config_seqno=3,
                        primaries=pp_seq_no + 2 - 1)

        check_audit_txn(txn=node.auditLedger.getBySeqNo(node.auditLedger.size),
                        view_no=view_no, pp_seq_no=pp_seq_no + 3,
                        seq_no=initial_seq_no + 3, txn_time=node.master_replica.last_accepted_pre_prepare_time,
                        txn_roots={CONFIG_LEDGER_ID: node.getLedger(CONFIG_LEDGER_ID).tree.root_hash},
                        state_roots={CONFIG_LEDGER_ID: node.getState(CONFIG_LEDGER_ID).committedHeadHash},
                        pool_size=initial_pool_size, domain_size=initial_domain_size,
                        config_size=initial_config_size,
                        last_pool_seqno=4,
                        last_domain_seqno=5,
                        last_config_seqno=None,
                        primaries=pp_seq_no + 3 - 1)
