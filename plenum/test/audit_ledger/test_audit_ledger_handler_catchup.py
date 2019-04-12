from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.test.audit_ledger.helper import check_audit_txn, do_apply_audit_txn, add_txns, DEFAULT_PRIMARIES

# BOTH TESTS NEED TO BE RUN TOGETHER AS THEY SHARE COMMITTED STATE
from plenum.test.testing_utils import FakeSomething


def test_revert_works_after_catchup(alh, db_manager,
                                    initial_domain_size, initial_pool_size, initial_config_size,
                                    initial_seq_no):
    size_before = alh.ledger.size

    # apply and commit batch
    do_apply_audit_txn(alh,
                       txns_count=7, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=3, pp_sq_no=35, txn_time=11111)
    txn_root_hash = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash = db_manager.get_state(DOMAIN_LEDGER_ID).headHash
    alh.commit_batch(FakeSomething())

    # add txns to audit ledger emulating catchup
    caughtup_txns = 5
    txns_per_batch = 2
    add_txns_to_audit(alh,
                      count=caughtup_txns,
                      ledger_id=POOL_LEDGER_ID,
                      txns_per_batch=txns_per_batch,
                      view_no=3,
                      initial_pp_seq_no=36,
                      pp_time=11222)
    alh.on_catchup_finished()

    txn_root_hash_1 = db_manager.get_ledger(POOL_LEDGER_ID).uncommitted_root_hash
    state_root_hash_1 = db_manager.get_state(POOL_LEDGER_ID).headHash

    # apply two new batches and revert them both
    do_apply_audit_txn(alh,
                       txns_count=3, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=3, pp_sq_no=45, txn_time=21111)
    txn_root_hash_2 = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash_2 = db_manager.get_state(DOMAIN_LEDGER_ID).headHash
    do_apply_audit_txn(alh,
                       txns_count=6, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=3, pp_sq_no=46, txn_time=21112)
    assert alh.ledger.uncommitted_size == alh.ledger.size + 2

    alh.post_batch_rejected(DOMAIN_LEDGER_ID)

    assert alh.ledger.uncommitted_size == alh.ledger.size + 1
    assert alh.ledger.size == size_before + 1 + caughtup_txns

    check_audit_txn(txn=alh.ledger.get_last_txn(),
                    view_no=3, pp_seq_no=45,
                    seq_no=initial_seq_no + 1 + caughtup_txns + 1, txn_time=21111,
                    txn_roots={DOMAIN_LEDGER_ID: txn_root_hash_2},
                    state_roots={DOMAIN_LEDGER_ID: state_root_hash_2},
                    pool_size=initial_pool_size + txns_per_batch * caughtup_txns,
                    domain_size=initial_domain_size + 7 + 3,
                    config_size=initial_config_size,
                    last_pool_seqno=initial_seq_no + 1 + caughtup_txns,
                    last_domain_seqno=None,
                    last_config_seqno=None,
                    primaries=caughtup_txns + 1)

    alh.post_batch_rejected(DOMAIN_LEDGER_ID)

    assert alh.ledger.uncommitted_size == alh.ledger.size
    assert alh.ledger.size == size_before + 1 + caughtup_txns

    check_audit_txn(txn=alh.ledger.get_last_txn(),
                    view_no=3, pp_seq_no=40,
                    seq_no=initial_seq_no + caughtup_txns + 1, txn_time=11222,
                    txn_roots={POOL_LEDGER_ID: txn_root_hash_1},
                    state_roots={POOL_LEDGER_ID: state_root_hash_1},
                    pool_size=initial_pool_size + txns_per_batch * caughtup_txns,
                    domain_size=initial_domain_size + 7,
                    config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=initial_seq_no + 1,
                    last_config_seqno=None,
                    primaries=caughtup_txns)


def test_commit_works_after_catchup(alh, db_manager,
                                    initial_domain_size, initial_pool_size, initial_config_size,
                                    initial_seq_no):
    size_before = alh.ledger.size

    # apply and commit batch
    do_apply_audit_txn(alh,
                       txns_count=7, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=3, pp_sq_no=35, txn_time=11111)
    txn_root_hash = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash = db_manager.get_state(DOMAIN_LEDGER_ID).headHash
    alh.commit_batch(FakeSomething())

    # add txns to audit ledger emulating catchup
    caughtup_txns = 5
    txns_per_batch = 2
    add_txns_to_audit(alh,
                      count=caughtup_txns,
                      ledger_id=POOL_LEDGER_ID,
                      txns_per_batch=txns_per_batch,
                      view_no=3,
                      initial_pp_seq_no=36,
                      pp_time=11222)
    alh.on_catchup_finished()

    # apply and commit new batch
    do_apply_audit_txn(alh,
                       txns_count=3, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=3, pp_sq_no=45, txn_time=21111)
    assert alh.ledger.uncommitted_size == alh.ledger.size + 1

    txn_root_hash = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash = db_manager.get_state(DOMAIN_LEDGER_ID).headHash
    alh.commit_batch(FakeSomething())

    assert alh.ledger.uncommitted_size == alh.ledger.size
    assert alh.ledger.size == size_before + 2 + caughtup_txns
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=3, pp_seq_no=45,
                    seq_no=initial_seq_no + 2 + caughtup_txns, txn_time=21111,
                    txn_roots={
                        DOMAIN_LEDGER_ID: txn_root_hash
                    },
                    state_roots={
                        DOMAIN_LEDGER_ID: state_root_hash
                    },
                    pool_size=initial_pool_size + txns_per_batch * caughtup_txns,
                    domain_size=initial_domain_size + 7 + 3,
                    config_size=initial_config_size,
                    last_pool_seqno=initial_seq_no + 1 + caughtup_txns,
                    last_domain_seqno=None,
                    last_config_seqno=None,
                    primaries=2 * (caughtup_txns + 1))


def add_txns_to_audit(alh, count, ledger_id, txns_per_batch, view_no, initial_pp_seq_no, pp_time):
    db_manager = alh.database_manager
    for i in range(count):
        add_txns(db_manager, ledger_id, txns_per_batch, pp_time)
        three_pc_batch = ThreePcBatch(ledger_id=ledger_id,
                                      inst_id=0,
                                      view_no=view_no,
                                      pp_seq_no=initial_pp_seq_no + i,
                                      pp_time=pp_time,
                                      state_root=db_manager.get_state(ledger_id).headHash,
                                      txn_root=db_manager.get_ledger(ledger_id).uncommitted_root_hash,
                                      primaries=DEFAULT_PRIMARIES,
                                      valid_digests=[])
        alh._add_to_ledger(three_pc_batch)
    alh.ledger.commitTxns(count)
