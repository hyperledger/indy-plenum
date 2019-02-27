from common.serializers.json_serializer import JsonSerializer
from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID, CONFIG_LEDGER_ID
from plenum.test.audit_ledger.helper import check_audit_txn, do_apply_audit_txn


def check_apply_audit_txn(alh,
                          txns_count, ledger_id,
                          view_no, pp_sq_no, txn_time, seq_no,
                          pool_size, domain_size, config_size,
                          last_pool_seqno, last_domain_seqno, last_config_seqno):
    db_manager = alh.database_manager
    uncommited_size_before = alh.ledger.uncommitted_size
    size_before = alh.ledger.size

    do_apply_audit_txn(alh,
                       txns_count=txns_count, ledger_id=ledger_id,
                       view_no=view_no, pp_sq_no=pp_sq_no, txn_time=txn_time)

    assert alh.ledger.uncommitted_size == uncommited_size_before + 1
    assert alh.ledger.size == size_before

    txn = alh.ledger.get_uncommitted_txns()[-1]
    check_audit_txn(txn=txn,
                    view_no=view_no, pp_seq_no=pp_sq_no,
                    seq_no=seq_no, txn_time=txn_time,
                    ledger_id=ledger_id,
                    txn_root=db_manager.get_ledger(ledger_id).uncommitted_root_hash,
                    state_root=db_manager.get_state(ledger_id).headHash,
                    pool_size=pool_size, domain_size=domain_size, config_size=config_size,
                    last_pool_seqno=last_pool_seqno,
                    last_domain_seqno=last_domain_seqno,
                    last_config_seqno=last_config_seqno)


def test_apply_audit_ledger_txn_pool_ledger(alh,
                                            initial_domain_size, initial_pool_size, initial_config_size):
    check_apply_audit_txn(alh=alh,
                          txns_count=10, ledger_id=POOL_LEDGER_ID,
                          view_no=1, pp_sq_no=10, txn_time=10000, seq_no=1,
                          pool_size=initial_pool_size + 10, domain_size=initial_domain_size,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=None, last_config_seqno=None)


def test_apply_audit_ledger_txn_domain_ledger(alh,
                                              initial_domain_size, initial_pool_size, initial_config_size):
    check_apply_audit_txn(alh=alh,
                          txns_count=15, ledger_id=DOMAIN_LEDGER_ID,
                          view_no=1, pp_sq_no=12, txn_time=10006, seq_no=1,
                          pool_size=initial_pool_size, domain_size=initial_domain_size + 15,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=None, last_config_seqno=None)


def test_apply_audit_ledger_txn_config_ledger(alh,
                                              initial_domain_size, initial_pool_size, initial_config_size):
    check_apply_audit_txn(alh=alh,
                          txns_count=20, ledger_id=CONFIG_LEDGER_ID,
                          view_no=1, pp_sq_no=15, txn_time=10008, seq_no=1,
                          pool_size=initial_pool_size, domain_size=initial_domain_size,
                          config_size=initial_config_size + 20,
                          last_pool_seqno=None, last_domain_seqno=None, last_config_seqno=None)


def test_apply_audit_ledger_txn_multi_ledger(alh,
                                             initial_domain_size, initial_pool_size, initial_config_size):
    # 1. add domain txn
    check_apply_audit_txn(alh=alh,
                          txns_count=10, ledger_id=DOMAIN_LEDGER_ID,
                          view_no=2, pp_sq_no=5, txn_time=500, seq_no=1,
                          pool_size=initial_pool_size, domain_size=initial_domain_size + 10,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=None, last_config_seqno=None)

    # 2. add pool txn
    check_apply_audit_txn(alh=alh,
                          txns_count=6, ledger_id=POOL_LEDGER_ID,
                          view_no=2, pp_sq_no=6, txn_time=502, seq_no=2,
                          pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 10,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=1, last_config_seqno=None)

    # 3. add config txn
    check_apply_audit_txn(alh=alh,
                          txns_count=8, ledger_id=CONFIG_LEDGER_ID,
                          view_no=2, pp_sq_no=7, txn_time=502, seq_no=3,
                          pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 10,
                          config_size=initial_config_size + 8,
                          last_pool_seqno=2, last_domain_seqno=1, last_config_seqno=None)

    # 4. add domain txn
    check_apply_audit_txn(alh=alh,
                          txns_count=2, ledger_id=DOMAIN_LEDGER_ID,
                          view_no=2, pp_sq_no=8, txn_time=550, seq_no=4,
                          pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 12,
                          config_size=initial_config_size + 8,
                          last_pool_seqno=2, last_domain_seqno=None, last_config_seqno=3)

    # 5. add domain txn
    check_apply_audit_txn(alh=alh,
                          txns_count=7, ledger_id=DOMAIN_LEDGER_ID,
                          view_no=2, pp_sq_no=9, txn_time=551, seq_no=5,
                          pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 19,
                          config_size=initial_config_size + 8,
                          last_pool_seqno=2, last_domain_seqno=None, last_config_seqno=3)

    # 6. add pool txn
    check_apply_audit_txn(alh=alh,
                          txns_count=5, ledger_id=POOL_LEDGER_ID,
                          view_no=2, pp_sq_no=10, txn_time=551, seq_no=6,
                          pool_size=initial_pool_size + 11, domain_size=initial_domain_size + 19,
                          config_size=initial_config_size + 8,
                          last_pool_seqno=None, last_domain_seqno=5, last_config_seqno=3)


def test_reject_batch(alh, db_manager,
                      initial_domain_size, initial_pool_size, initial_config_size):
    uncommited_size_before = alh.ledger.uncommitted_size
    size_before = alh.ledger.size

    do_apply_audit_txn(alh,
                       txns_count=5, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=3, pp_sq_no=37, txn_time=11112)
    txn_root_hash_1 = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash_1 = db_manager.get_state(DOMAIN_LEDGER_ID).headHash

    do_apply_audit_txn(alh,
                       txns_count=6, ledger_id=POOL_LEDGER_ID,
                       view_no=3, pp_sq_no=38, txn_time=11113)
    txn_root_hash_2 = db_manager.get_ledger(POOL_LEDGER_ID).uncommitted_root_hash
    state_root_hash_2 = db_manager.get_state(POOL_LEDGER_ID).headHash

    do_apply_audit_txn(alh,
                       txns_count=7, ledger_id=CONFIG_LEDGER_ID,
                       view_no=3, pp_sq_no=39, txn_time=11114)
    txn_root_hash_3 = db_manager.get_ledger(CONFIG_LEDGER_ID).uncommitted_root_hash
    state_root_hash_3 = db_manager.get_state(CONFIG_LEDGER_ID).headHash

    do_apply_audit_txn(alh,
                       txns_count=8, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=3, pp_sq_no=40, txn_time=11115)

    assert alh.ledger.uncommitted_size == uncommited_size_before + 4
    assert alh.ledger.size == size_before

    alh.post_batch_rejected(DOMAIN_LEDGER_ID)
    assert alh.ledger.uncommitted_size == uncommited_size_before + 3
    assert alh.ledger.size == size_before
    check_audit_txn(txn=alh.ledger.get_last_txn(),
                    view_no=3, pp_seq_no=39,
                    seq_no=3, txn_time=11114,
                    ledger_id=CONFIG_LEDGER_ID,
                    txn_root=txn_root_hash_3,
                    state_root=state_root_hash_3,
                    pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 5,
                    config_size=initial_config_size + 7,
                    last_pool_seqno=2,
                    last_domain_seqno=1,
                    last_config_seqno=None)

    alh.post_batch_rejected(DOMAIN_LEDGER_ID)
    assert alh.ledger.uncommitted_size == uncommited_size_before + 2
    assert alh.ledger.size == size_before
    check_audit_txn(txn=alh.ledger.get_last_txn(),
                    view_no=3, pp_seq_no=38,
                    seq_no=2, txn_time=11113,
                    ledger_id=POOL_LEDGER_ID,
                    txn_root=txn_root_hash_2,
                    state_root=state_root_hash_2,
                    pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 5,
                    config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=1,
                    last_config_seqno=None)

    alh.post_batch_rejected(DOMAIN_LEDGER_ID)
    assert alh.ledger.uncommitted_size == uncommited_size_before + 1
    assert alh.ledger.size == size_before
    check_audit_txn(txn=alh.ledger.get_last_txn(),
                    view_no=3, pp_seq_no=37,
                    seq_no=1, txn_time=11112,
                    ledger_id=DOMAIN_LEDGER_ID,
                    txn_root=txn_root_hash_1,
                    state_root=state_root_hash_1,
                    pool_size=initial_pool_size, domain_size=initial_domain_size + 5, config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=None,
                    last_config_seqno=None)

    alh.post_batch_rejected(DOMAIN_LEDGER_ID)
    assert alh.ledger.uncommitted_size == uncommited_size_before
    assert alh.ledger.size == size_before
    assert alh.ledger.get_last_txn() is None


def test_transform_txn_for_catchup_rep(alh, db_manager,
                                       initial_domain_size, initial_pool_size, initial_config_size):
    do_apply_audit_txn(alh,
                       txns_count=10, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=0, pp_sq_no=1, txn_time=10000,
                       has_audit_txn=True)

    audit_txn_after_serialization = \
        JsonSerializer.loads(
            JsonSerializer.dumps(
                alh.ledger.get_last_txn()
            )
        )

    transformed_audit_txn = alh.transform_txn_for_ledger(audit_txn_after_serialization)
    check_audit_txn(txn=transformed_audit_txn,
                    view_no=0, pp_seq_no=1,
                    seq_no=1, txn_time=10000,
                    ledger_id=DOMAIN_LEDGER_ID,
                    txn_root=db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash,
                    state_root=db_manager.get_state(DOMAIN_LEDGER_ID).headHash,
                    pool_size=initial_pool_size,
                    domain_size=initial_domain_size + 10,
                    config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=None,
                    last_config_seqno=None)


def test_commit_one_batch(alh, db_manager,
                          initial_domain_size, initial_pool_size, initial_config_size,
                          initial_seq_no):
    size_before = alh.ledger.size
    do_apply_audit_txn(alh,
                       txns_count=7, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=3, pp_sq_no=35, txn_time=11111)
    txn_root_hash = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash = db_manager.get_state(DOMAIN_LEDGER_ID).headHash
    alh.commit_batch(DOMAIN_LEDGER_ID, 7, state_root_hash, txn_root_hash, 11111)

    assert alh.ledger.uncommitted_size == alh.ledger.size
    assert alh.ledger.size == size_before + 1
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=3, pp_seq_no=35,
                    seq_no=initial_seq_no + 1, txn_time=11111,
                    ledger_id=DOMAIN_LEDGER_ID,
                    txn_root=txn_root_hash,
                    state_root=state_root_hash,
                    pool_size=initial_pool_size, domain_size=initial_domain_size + 7, config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=None,
                    last_config_seqno=None)


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

    do_apply_audit_txn(alh,
                       txns_count=15, ledger_id=POOL_LEDGER_ID,
                       view_no=3, pp_sq_no=36, txn_time=11112)

    # reject 2d batch
    alh.post_batch_rejected(POOL_LEDGER_ID)
    assert alh.ledger.uncommitted_size == alh.ledger.size + 1
    assert alh.ledger.size == size_before

    # commit 1st batch
    alh.commit_batch(DOMAIN_LEDGER_ID, 7, state_root_hash_1, txn_root_hash_1, 11111)
    assert alh.ledger.uncommitted_size == alh.ledger.size
    assert alh.ledger.size == size_before + 1
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=3, pp_seq_no=35,
                    seq_no=initial_seq_no + 1, txn_time=11111,
                    ledger_id=DOMAIN_LEDGER_ID,
                    txn_root=txn_root_hash_1,
                    state_root=state_root_hash_1,
                    pool_size=initial_pool_size, domain_size=initial_domain_size + 7, config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=None,
                    last_config_seqno=None)


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
    alh.commit_batch(DOMAIN_LEDGER_ID, 7, state_root_hash_1, txn_root_hash_1, 11111)
    assert alh.ledger.uncommitted_size == alh.ledger.size + 4
    assert alh.ledger.size == size_before + 1
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=3, pp_seq_no=35,
                    seq_no=initial_seq_no + 1, txn_time=11111,
                    ledger_id=DOMAIN_LEDGER_ID,
                    txn_root=txn_root_hash_1,
                    state_root=state_root_hash_1,
                    pool_size=initial_pool_size, domain_size=initial_domain_size + 7, config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=None,
                    last_config_seqno=None)

    # commit 2d batch
    alh.commit_batch(POOL_LEDGER_ID, 15, state_root_hash_2, txn_root_hash_2, 11112)
    assert alh.ledger.uncommitted_size == alh.ledger.size + 3
    assert alh.ledger.size == size_before + 2
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=3, pp_seq_no=36,
                    seq_no=initial_seq_no + 2, txn_time=11112,
                    ledger_id=POOL_LEDGER_ID,
                    txn_root=txn_root_hash_2,
                    state_root=state_root_hash_2,
                    pool_size=initial_pool_size + 15, domain_size=initial_domain_size + 7,
                    config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=initial_seq_no + 1,
                    last_config_seqno=None)

    # commit 3d batch
    alh.commit_batch(CONFIG_LEDGER_ID, 5, state_root_hash_3, txn_root_hash_3, 11112)
    assert alh.ledger.uncommitted_size == alh.ledger.size + 2
    assert alh.ledger.size == size_before + 3
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=3, pp_seq_no=37,
                    seq_no=initial_seq_no + 3, txn_time=11112,
                    ledger_id=CONFIG_LEDGER_ID,
                    txn_root=txn_root_hash_3,
                    state_root=state_root_hash_3,
                    pool_size=initial_pool_size + 15, domain_size=initial_domain_size + 7,
                    config_size=initial_config_size + 5,
                    last_pool_seqno=initial_seq_no + 2,
                    last_domain_seqno=initial_seq_no + 1,
                    last_config_seqno=None)

    # commit 4th batch
    alh.commit_batch(DOMAIN_LEDGER_ID, 10, state_root_hash_4, txn_root_hash_4, 11115)
    assert alh.ledger.uncommitted_size == alh.ledger.size + 1
    assert alh.ledger.size == size_before + 4
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=4, pp_seq_no=1,
                    seq_no=initial_seq_no + 4, txn_time=11115,
                    ledger_id=DOMAIN_LEDGER_ID,
                    txn_root=txn_root_hash_4,
                    state_root=state_root_hash_4,
                    pool_size=initial_pool_size + 15, domain_size=initial_domain_size + 7 + 10,
                    config_size=initial_config_size + 5,
                    last_pool_seqno=initial_seq_no + 2,
                    last_domain_seqno=None,
                    last_config_seqno=initial_seq_no + 3)

    # commit 5th batch
    alh.commit_batch(DOMAIN_LEDGER_ID, 20, state_root_hash_5, txn_root_hash_5, 11119)
    assert alh.ledger.uncommitted_size == alh.ledger.size
    assert alh.ledger.size == size_before + 5
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=4, pp_seq_no=2,
                    seq_no=initial_seq_no + 5, txn_time=11119,
                    ledger_id=DOMAIN_LEDGER_ID,
                    txn_root=txn_root_hash_5,
                    state_root=state_root_hash_5,
                    pool_size=initial_pool_size + 15, domain_size=initial_domain_size + 7 + 10 + 20,
                    config_size=initial_config_size + 5,
                    last_pool_seqno=initial_seq_no + 2,
                    last_domain_seqno=None,
                    last_config_seqno=initial_seq_no + 3)


def test_audit_not_applied_if_pre_prepare_doesnt_have_audit(alh):
    size_before = alh.ledger.size
    uncommited_size_before = alh.ledger.uncommitted_size

    do_apply_audit_txn(alh,
                       txns_count=10, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=0, pp_sq_no=1, txn_time=10000,
                       has_audit_txn=False)

    assert alh.ledger.uncommitted_size == uncommited_size_before
    assert alh.ledger.size == size_before
    assert alh.ledger.size == alh.ledger.uncommitted_size


def test_audit_not_committed_if_pre_prepare_doesnt_have_audit(alh, db_manager):
    size_before = alh.ledger.size
    uncommited_size_before = alh.ledger.uncommitted_size

    do_apply_audit_txn(alh,
                       txns_count=10, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=0, pp_sq_no=1, txn_time=10000,
                       has_audit_txn=False)
    txn_root_hash_1 = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash_1 = db_manager.get_state(DOMAIN_LEDGER_ID).headHash

    do_apply_audit_txn(alh,
                       txns_count=15, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=0, pp_sq_no=2, txn_time=10000,
                       has_audit_txn=True)

    # commit the first batch without audit txns
    alh.commit_batch(DOMAIN_LEDGER_ID, 10, state_root_hash_1, txn_root_hash_1, 10000)

    assert alh.ledger.uncommitted_size == uncommited_size_before + 1
    assert alh.ledger.size == size_before


def test_audit_not_reverted_if_pre_prepare_doesnt_have_audit(alh, db_manager):
    do_apply_audit_txn(alh,
                       txns_count=10, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=0, pp_sq_no=1, txn_time=10000,
                       has_audit_txn=True)
    size_after_1st = alh.ledger.size
    uncommited_size_after_1st = alh.ledger.uncommitted_size

    do_apply_audit_txn(alh,
                       txns_count=15, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=0, pp_sq_no=2, txn_time=10000,
                       has_audit_txn=False)

    # revert the 2d batch without audit
    alh.post_batch_rejected(DOMAIN_LEDGER_ID)

    assert alh.ledger.uncommitted_size == uncommited_size_after_1st
    assert alh.ledger.size == size_after_1st
