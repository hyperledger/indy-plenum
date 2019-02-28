from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID
from plenum.test.audit_ledger.helper import check_audit_ledger_updated, check_audit_txn
from plenum.test.bls.helper import sdk_change_bls_key
from plenum.test.helper import sdk_send_random_and_check


def test_audit_ledger_updated_after_ordering(looper, txnPoolNodeSet,
                                             sdk_pool_handle, sdk_wallet_client, sdk_wallet_stewards,
                                             initial_domain_size, initial_pool_size, initial_config_size,
                                             view_no, pp_seq_no,
                                             initial_seq_no):
    '''
    Order 2 domain txns, 2 pool txns, and then 1 domain txn
    Check that audit ledger is correctly updated in all cases
    '''
    # 1st domain txn
    audit_size_initial = [node.auditLedger.size for node in txnPoolNodeSet]
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    check_audit_ledger_updated(audit_size_initial, txnPoolNodeSet,
                               audit_txns_added=1)
    for node in txnPoolNodeSet:
        check_audit_txn(txn=node.auditLedger.get_last_txn(),
                        view_no=view_no, pp_seq_no=pp_seq_no + 1,
                        seq_no=initial_seq_no + 1, txn_time=node.master_replica.last_accepted_pre_prepare_time,
                        ledger_id=DOMAIN_LEDGER_ID,
                        txn_root=node.getLedger(DOMAIN_LEDGER_ID).tree.root_hash,
                        state_root=node.getState(DOMAIN_LEDGER_ID).committedHeadHash,
                        pool_size=initial_pool_size, domain_size=initial_domain_size + 1,
                        config_size=initial_config_size,
                        last_pool_seqno=None,
                        last_domain_seqno=None,
                        last_config_seqno=None)

    # 2d domain txn
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    check_audit_ledger_updated(audit_size_initial, txnPoolNodeSet,
                               audit_txns_added=2)
    for node in txnPoolNodeSet:
        check_audit_txn(txn=node.auditLedger.get_last_txn(),
                        view_no=view_no, pp_seq_no=pp_seq_no + 2,
                        seq_no=initial_seq_no + 2, txn_time=node.master_replica.last_accepted_pre_prepare_time,
                        ledger_id=DOMAIN_LEDGER_ID,
                        txn_root=node.getLedger(DOMAIN_LEDGER_ID).tree.root_hash,
                        state_root=node.getState(DOMAIN_LEDGER_ID).committedHeadHash,
                        pool_size=initial_pool_size, domain_size=initial_domain_size + 2,
                        config_size=initial_config_size,
                        last_pool_seqno=None,
                        last_domain_seqno=None,
                        last_config_seqno=None)

    # 1st pool txn
    sdk_change_bls_key(looper, txnPoolNodeSet,
                       txnPoolNodeSet[3],
                       sdk_pool_handle,
                       sdk_wallet_stewards[3],
                       check_functional=False)
    check_audit_ledger_updated(audit_size_initial, txnPoolNodeSet,
                               audit_txns_added=3)
    for node in txnPoolNodeSet:
        check_audit_txn(txn=node.auditLedger.get_last_txn(),
                        view_no=view_no, pp_seq_no=pp_seq_no + 3,
                        seq_no=initial_seq_no + 3, txn_time=node.master_replica.last_accepted_pre_prepare_time,
                        ledger_id=POOL_LEDGER_ID,
                        txn_root=node.getLedger(POOL_LEDGER_ID).tree.root_hash,
                        state_root=node.getState(POOL_LEDGER_ID).committedHeadHash,
                        pool_size=initial_pool_size + 1, domain_size=initial_domain_size + 2,
                        config_size=initial_config_size,
                        last_pool_seqno=None,
                        last_domain_seqno=2,
                        last_config_seqno=None)

    # 2d pool txn
    sdk_change_bls_key(looper, txnPoolNodeSet,
                       txnPoolNodeSet[3],
                       sdk_pool_handle,
                       sdk_wallet_stewards[3],
                       check_functional=False)
    check_audit_ledger_updated(audit_size_initial, txnPoolNodeSet,
                               audit_txns_added=4)
    for node in txnPoolNodeSet:
        check_audit_txn(txn=node.auditLedger.get_last_txn(),
                        view_no=view_no, pp_seq_no=pp_seq_no + 4,
                        seq_no=initial_seq_no + 4, txn_time=node.master_replica.last_accepted_pre_prepare_time,
                        ledger_id=POOL_LEDGER_ID,
                        txn_root=node.getLedger(POOL_LEDGER_ID).tree.root_hash,
                        state_root=node.getState(POOL_LEDGER_ID).committedHeadHash,
                        pool_size=initial_pool_size + 2, domain_size=initial_domain_size + 2,
                        config_size=initial_config_size,
                        last_pool_seqno=None,
                        last_domain_seqno=2,
                        last_config_seqno=None)

    # one more domain txn
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    check_audit_ledger_updated(audit_size_initial, txnPoolNodeSet,
                               audit_txns_added=5)
    for node in txnPoolNodeSet:
        check_audit_txn(txn=node.auditLedger.get_last_txn(),
                        view_no=view_no, pp_seq_no=pp_seq_no + 5,
                        seq_no=initial_seq_no + 5, txn_time=node.master_replica.last_accepted_pre_prepare_time,
                        ledger_id=DOMAIN_LEDGER_ID,
                        txn_root=node.getLedger(DOMAIN_LEDGER_ID).tree.root_hash,
                        state_root=node.getState(DOMAIN_LEDGER_ID).committedHeadHash,
                        pool_size=initial_pool_size + 2, domain_size=initial_domain_size + 3,
                        config_size=initial_config_size,
                        last_pool_seqno=4,
                        last_domain_seqno=None,
                        last_config_seqno=None)
