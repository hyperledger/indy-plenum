from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID, CONFIG_LEDGER_ID
from plenum.test.audit_ledger.helper import check_audit_txn, do_apply_audit_txn, DEFAULT_PRIMARIES, DEFAULT_NODE_REG

# BOTH TESTS NEED TO BE RUN TOGETHER AS THEY SHARE COMMITTED STATE
from plenum.test.testing_utils import FakeSomething


def test_apply_revert_commit(alh, db_manager,
                             initial_domain_size, initial_pool_size, initial_config_size,
                             initial_seq_no):
    size_before = alh.ledger.size

    # apply 2 batches
    do_apply_audit_txn(alh,
                       txns_count=7, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=3, pp_sq_no=35, txn_time=11111)
    txn_root_hash_1 = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash_1 = db_manager.get_state(DOMAIN_LEDGER_ID).headHash
    txn_root_hash_2 = db_manager.get_ledger(POOL_LEDGER_ID).uncommitted_root_hash
    state_root_hash_2 = db_manager.get_state(POOL_LEDGER_ID).headHash

    do_apply_audit_txn(alh,
                       txns_count=15, ledger_id=POOL_LEDGER_ID,
                       view_no=3, pp_sq_no=36, txn_time=11112)

    # reject 2d batch
    alh.post_batch_rejected(POOL_LEDGER_ID)
    assert alh.ledger.uncommitted_size == alh.ledger.size + 1
    assert alh.ledger.size == size_before

    # commit 1st batch
    alh.commit_batch(FakeSomething())
    assert alh.ledger.uncommitted_size == alh.ledger.size
    assert alh.ledger.size == size_before + 1
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=3, pp_seq_no=35,
                    seq_no=initial_seq_no + 1, txn_time=11111,
                    txn_roots={
                        DOMAIN_LEDGER_ID: txn_root_hash_1,
                        POOL_LEDGER_ID: txn_root_hash_2
                    },
                    state_roots={
                        DOMAIN_LEDGER_ID: state_root_hash_1,
                        POOL_LEDGER_ID: state_root_hash_2
                    },
                    pool_size=initial_pool_size, domain_size=initial_domain_size + 7, config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=None,
                    last_config_seqno=None,
                    primaries=DEFAULT_PRIMARIES,
                    node_reg=DEFAULT_NODE_REG)


def test_commit_multiple_batches(alh, db_manager,
                                 initial_domain_size, initial_pool_size, initial_config_size,
                                 initial_seq_no):
    size_before = alh.ledger.size

    # apply 5 3PC batches
    do_apply_audit_txn(alh,
                       txns_count=7, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=3, pp_sq_no=35, txn_time=11111)
    txn_root_hash_1 = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash_1 = db_manager.get_state(DOMAIN_LEDGER_ID).headHash

    do_apply_audit_txn(alh,
                       txns_count=15, ledger_id=POOL_LEDGER_ID,
                       view_no=3, pp_sq_no=36, txn_time=11112)
    txn_root_hash_2 = db_manager.get_ledger(POOL_LEDGER_ID).uncommitted_root_hash
    state_root_hash_2 = db_manager.get_state(POOL_LEDGER_ID).headHash

    do_apply_audit_txn(alh,
                       txns_count=5, ledger_id=CONFIG_LEDGER_ID,
                       view_no=3, pp_sq_no=37, txn_time=11112)
    txn_root_hash_3 = db_manager.get_ledger(CONFIG_LEDGER_ID).uncommitted_root_hash
    state_root_hash_3 = db_manager.get_state(CONFIG_LEDGER_ID).headHash

    do_apply_audit_txn(alh,
                       txns_count=10, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=4, pp_sq_no=1, txn_time=11115)
    txn_root_hash_4 = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash_4 = db_manager.get_state(DOMAIN_LEDGER_ID).headHash

    do_apply_audit_txn(alh,
                       txns_count=20, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=4, pp_sq_no=2, txn_time=11119)
    txn_root_hash_5 = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash_5 = db_manager.get_state(DOMAIN_LEDGER_ID).headHash

    assert alh.ledger.uncommitted_size == alh.ledger.size + 5

    # commit 1st batch
    alh.commit_batch(FakeSomething())
    assert alh.ledger.uncommitted_size == alh.ledger.size + 4
    assert alh.ledger.size == size_before + 1
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=3, pp_seq_no=35,
                    seq_no=initial_seq_no + 1, txn_time=11111,
                    txn_roots={
                        DOMAIN_LEDGER_ID: txn_root_hash_1,
                    },
                    state_roots={
                        DOMAIN_LEDGER_ID: state_root_hash_1,
                    },
                    pool_size=initial_pool_size, domain_size=initial_domain_size + 7, config_size=initial_config_size,
                    last_pool_seqno=1,
                    last_domain_seqno=None,
                    last_config_seqno=None,
                    primaries=1,
                    node_reg=1)

    # commit 2d batch
    alh.commit_batch(FakeSomething())
    assert alh.ledger.uncommitted_size == alh.ledger.size + 3
    assert alh.ledger.size == size_before + 2
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=3, pp_seq_no=36,
                    seq_no=initial_seq_no + 2, txn_time=11112,
                    txn_roots={
                        POOL_LEDGER_ID: txn_root_hash_2,
                    },
                    state_roots={
                        POOL_LEDGER_ID: state_root_hash_2,
                    },
                    pool_size=initial_pool_size + 15, domain_size=initial_domain_size + 7,
                    config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=initial_seq_no + 1,
                    last_config_seqno=None,
                    primaries=2,
                    node_reg=2)

    # commit 3d batch
    alh.commit_batch(FakeSomething())
    assert alh.ledger.uncommitted_size == alh.ledger.size + 2
    assert alh.ledger.size == size_before + 3
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=3, pp_seq_no=37,
                    seq_no=initial_seq_no + 3, txn_time=11112,
                    txn_roots={
                        CONFIG_LEDGER_ID: txn_root_hash_3,
                    },
                    state_roots={
                        CONFIG_LEDGER_ID: state_root_hash_3,
                    },
                    pool_size=initial_pool_size + 15, domain_size=initial_domain_size + 7,
                    config_size=initial_config_size + 5,
                    last_pool_seqno=initial_seq_no + 2,
                    last_domain_seqno=initial_seq_no + 1,
                    last_config_seqno=None,
                    primaries=3,
                    node_reg=3)

    # commit 4th batch
    alh.commit_batch(FakeSomething())
    assert alh.ledger.uncommitted_size == alh.ledger.size + 1
    assert alh.ledger.size == size_before + 4
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=4, pp_seq_no=1,
                    seq_no=initial_seq_no + 4, txn_time=11115,
                    txn_roots={
                        DOMAIN_LEDGER_ID: txn_root_hash_4,
                    },
                    state_roots={
                        DOMAIN_LEDGER_ID: state_root_hash_4,
                    },
                    pool_size=initial_pool_size + 15, domain_size=initial_domain_size + 7 + 10,
                    config_size=initial_config_size + 5,
                    last_pool_seqno=initial_seq_no + 2,
                    last_domain_seqno=None,
                    last_config_seqno=initial_seq_no + 3,
                    primaries=4,
                    node_reg=4)

    # commit 5th batch
    alh.commit_batch(FakeSomething())
    assert alh.ledger.uncommitted_size == alh.ledger.size
    assert alh.ledger.size == size_before + 5
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=4, pp_seq_no=2,
                    seq_no=initial_seq_no + 5, txn_time=11119,
                    txn_roots={
                        DOMAIN_LEDGER_ID: txn_root_hash_5,
                    },
                    state_roots={
                        DOMAIN_LEDGER_ID: state_root_hash_5,
                    },
                    pool_size=initial_pool_size + 15, domain_size=initial_domain_size + 7 + 10 + 20,
                    config_size=initial_config_size + 5,
                    last_pool_seqno=initial_seq_no + 2,
                    last_domain_seqno=None,
                    last_config_seqno=initial_seq_no + 3,
                    primaries=5,
                    node_reg=5)
