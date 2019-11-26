from common.serializers.json_serializer import JsonSerializer
from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID, CONFIG_LEDGER_ID
from plenum.test.audit_ledger.helper import check_audit_txn, do_apply_audit_txn, DEFAULT_PRIMARIES, DEFAULT_NODE_REG
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.main import integrate_plugin_in_node
from plenum.test.testing_utils import FakeSomething


def check_apply_audit_txn(alh,
                          txns_count, ledger_ids,
                          view_no, pp_sq_no, txn_time, seq_no,
                          pool_size, domain_size, config_size,
                          last_pool_seqno, last_domain_seqno, last_config_seqno,
                          primaries, other_sizes={},
                          original_view_no=None, digest='',
                          node_reg=DEFAULT_NODE_REG):
    db_manager = alh.database_manager
    uncommited_size_before = alh.ledger.uncommitted_size
    size_before = alh.ledger.size

    do_apply_audit_txn(alh,
                       txns_count=txns_count, ledger_id=ledger_ids[0],
                       view_no=view_no, pp_sq_no=pp_sq_no, txn_time=txn_time,
                       original_view_no=original_view_no, digest=digest, nod_reg=node_reg)

    assert alh.ledger.uncommitted_size == uncommited_size_before + 1
    assert alh.ledger.size == size_before

    txn = alh.ledger.get_uncommitted_txns()[-1]
    expected_view_no = original_view_no if original_view_no is not None else view_no
    check_audit_txn(txn=txn,
                    view_no=expected_view_no, pp_seq_no=pp_sq_no,
                    seq_no=seq_no, txn_time=txn_time,
                    txn_roots={
                        ledger_id: db_manager.get_ledger(ledger_id).uncommitted_root_hash for ledger_id in ledger_ids
                    },
                    state_roots={
                        ledger_id: db_manager.get_state(ledger_id).headHash for ledger_id in ledger_ids
                    },
                    pool_size=pool_size, domain_size=domain_size, config_size=config_size,
                    last_pool_seqno=last_pool_seqno,
                    last_domain_seqno=last_domain_seqno,
                    last_config_seqno=last_config_seqno,
                    primaries=primaries,
                    node_reg=node_reg,
                    other_sizes=other_sizes,
                    digest=digest)


def test_apply_audit_ledger_txn_pool_ledger(alh,
                                            initial_domain_size, initial_pool_size, initial_config_size):
    check_apply_audit_txn(alh=alh,
                          txns_count=10, ledger_ids=[POOL_LEDGER_ID, DOMAIN_LEDGER_ID],
                          view_no=1, pp_sq_no=10, txn_time=10000, seq_no=1,
                          pool_size=initial_pool_size + 10, domain_size=initial_domain_size,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=None, last_config_seqno=None,
                          primaries=DEFAULT_PRIMARIES,
                          original_view_no=0,
                          node_reg=DEFAULT_NODE_REG)


def test_apply_audit_ledger_txn_domain_ledger(alh,
                                              initial_domain_size, initial_pool_size, initial_config_size):
    check_apply_audit_txn(alh=alh,
                          txns_count=15, ledger_ids=[DOMAIN_LEDGER_ID, POOL_LEDGER_ID],
                          view_no=1, pp_sq_no=12, txn_time=10006, seq_no=1,
                          pool_size=initial_pool_size, domain_size=initial_domain_size + 15,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=None, last_config_seqno=None,
                          primaries=DEFAULT_PRIMARIES,
                          original_view_no=0,
                          node_reg=DEFAULT_NODE_REG)


def test_apply_audit_ledger_txn_config_ledger(alh,
                                              initial_domain_size, initial_pool_size, initial_config_size):
    check_apply_audit_txn(alh=alh,
                          txns_count=20, ledger_ids=[CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID, POOL_LEDGER_ID],
                          view_no=1, pp_sq_no=15, txn_time=10008, seq_no=1,
                          pool_size=initial_pool_size, domain_size=initial_domain_size,
                          config_size=initial_config_size + 20,
                          last_pool_seqno=None, last_domain_seqno=None, last_config_seqno=None,
                          primaries=DEFAULT_PRIMARIES,
                          original_view_no=0,
                          node_reg=DEFAULT_NODE_REG)


def test_apply_audit_ledger_txn_multi_ledger(alh,
                                             initial_domain_size, initial_pool_size, initial_config_size):
    # 1. add domain txn
    check_apply_audit_txn(alh=alh,
                          txns_count=10, ledger_ids=[DOMAIN_LEDGER_ID, POOL_LEDGER_ID],
                          view_no=2, pp_sq_no=5, txn_time=500, seq_no=1,
                          pool_size=initial_pool_size, domain_size=initial_domain_size + 10,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=None, last_config_seqno=None,
                          primaries=DEFAULT_PRIMARIES,
                          digest='pp_digest_1',
                          node_reg=None)  # make it None to emulate audit txns without node reg yet

    # 2. add pool txn
    check_apply_audit_txn(alh=alh,
                          txns_count=6, ledger_ids=[POOL_LEDGER_ID],
                          view_no=2, pp_sq_no=6, txn_time=502, seq_no=2,
                          pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 10,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=1, last_config_seqno=None,
                          primaries=1,
                          digest='pp_digest_2',
                          node_reg=['Alpha', 'Beta', 'Gamma', 'Delta', 'Eta'])

    # 3. add config txn
    check_apply_audit_txn(alh=alh,
                          txns_count=8, ledger_ids=[CONFIG_LEDGER_ID],
                          view_no=2, pp_sq_no=7, txn_time=502, seq_no=3,
                          pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 10,
                          config_size=initial_config_size + 8,
                          last_pool_seqno=2, last_domain_seqno=1, last_config_seqno=None,
                          primaries=2,
                          digest='pp_digest_3',
                          node_reg=1)

    # 4. add domain txn
    check_apply_audit_txn(alh=alh,
                          txns_count=2, ledger_ids=[DOMAIN_LEDGER_ID],
                          view_no=2, pp_sq_no=8, txn_time=550, seq_no=4,
                          pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 12,
                          config_size=initial_config_size + 8,
                          last_pool_seqno=2, last_domain_seqno=None, last_config_seqno=3,
                          primaries=3,
                          digest='pp_digest_4',
                          node_reg=2)

    # 5. add domain txn
    check_apply_audit_txn(alh=alh,
                          txns_count=7, ledger_ids=[DOMAIN_LEDGER_ID],
                          view_no=2, pp_sq_no=9, txn_time=551, seq_no=5,
                          pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 19,
                          config_size=initial_config_size + 8,
                          last_pool_seqno=2, last_domain_seqno=None, last_config_seqno=3,
                          primaries=4,
                          digest='pp_digest_5',
                          node_reg=3)

    # 6. add pool txn
    check_apply_audit_txn(alh=alh,
                          txns_count=5, ledger_ids=[POOL_LEDGER_ID],
                          view_no=2, pp_sq_no=10, txn_time=551, seq_no=6,
                          pool_size=initial_pool_size + 11, domain_size=initial_domain_size + 19,
                          config_size=initial_config_size + 8,
                          last_pool_seqno=None, last_domain_seqno=5, last_config_seqno=3,
                          primaries=5,
                          digest='pp_digest_6',
                          node_reg=['Beta', 'Gamma', 'Delta', 'Eta', 'Alpha'])


def test_reject_batch(alh, db_manager,
                      initial_domain_size, initial_pool_size, initial_config_size):
    uncommited_size_before = alh.ledger.uncommitted_size
    size_before = alh.ledger.size

    do_apply_audit_txn(alh,
                       txns_count=5, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=3, pp_sq_no=37, txn_time=11112)
    txn_root_hash_1 = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash_1 = db_manager.get_state(DOMAIN_LEDGER_ID).headHash

    txn_root_hash_2_pre = db_manager.get_ledger(POOL_LEDGER_ID).uncommitted_root_hash
    state_root_hash_2_pre = db_manager.get_state(POOL_LEDGER_ID).headHash

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
                    txn_roots={
                        CONFIG_LEDGER_ID: txn_root_hash_3,
                    },
                    state_roots={
                        CONFIG_LEDGER_ID: state_root_hash_3,
                    },
                    pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 5,
                    config_size=initial_config_size + 7,
                    last_pool_seqno=2,
                    last_domain_seqno=1,
                    last_config_seqno=None,
                    primaries=2,
                    node_reg=2)

    alh.post_batch_rejected(DOMAIN_LEDGER_ID)
    assert alh.ledger.uncommitted_size == uncommited_size_before + 2
    assert alh.ledger.size == size_before
    check_audit_txn(txn=alh.ledger.get_last_txn(),
                    view_no=3, pp_seq_no=38,
                    seq_no=2, txn_time=11113,
                    txn_roots={
                        POOL_LEDGER_ID: txn_root_hash_2,
                    },
                    state_roots={
                        POOL_LEDGER_ID: state_root_hash_2,
                    },
                    pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 5,
                    config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=1,
                    last_config_seqno=None,
                    primaries=1,
                    node_reg=1)

    alh.post_batch_rejected(DOMAIN_LEDGER_ID)
    assert alh.ledger.uncommitted_size == uncommited_size_before + 1
    assert alh.ledger.size == size_before
    check_audit_txn(txn=alh.ledger.get_last_txn(),
                    view_no=3, pp_seq_no=37,
                    seq_no=1, txn_time=11112,
                    txn_roots={
                        DOMAIN_LEDGER_ID: txn_root_hash_1,
                        POOL_LEDGER_ID: txn_root_hash_2_pre
                    },
                    state_roots={
                        DOMAIN_LEDGER_ID: state_root_hash_1,
                        POOL_LEDGER_ID: state_root_hash_2_pre
                    },
                    pool_size=initial_pool_size, domain_size=initial_domain_size + 5, config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=None,
                    last_config_seqno=None,
                    primaries=DEFAULT_PRIMARIES,
                    node_reg=DEFAULT_NODE_REG)

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
                    txn_roots={
                        DOMAIN_LEDGER_ID: db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash,
                        POOL_LEDGER_ID: db_manager.get_ledger(POOL_LEDGER_ID).uncommitted_root_hash,
                    },
                    state_roots={
                        DOMAIN_LEDGER_ID: db_manager.get_state(DOMAIN_LEDGER_ID).headHash,
                        POOL_LEDGER_ID: db_manager.get_state(POOL_LEDGER_ID).headHash,
                    },
                    pool_size=initial_pool_size,
                    domain_size=initial_domain_size + 10,
                    config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=None,
                    last_config_seqno=None,
                    primaries=DEFAULT_PRIMARIES,
                    node_reg=DEFAULT_NODE_REG)


def test_commit_one_batch(alh, db_manager,
                          initial_domain_size, initial_pool_size, initial_config_size,
                          initial_seq_no):
    size_before = alh.ledger.size
    digest = '123/0digest'
    do_apply_audit_txn(alh,
                       txns_count=7, ledger_id=DOMAIN_LEDGER_ID,
                       view_no=3, pp_sq_no=35, txn_time=11111,
                       digest=digest)
    txn_root_hash = db_manager.get_ledger(DOMAIN_LEDGER_ID).uncommitted_root_hash
    state_root_hash = db_manager.get_state(DOMAIN_LEDGER_ID).headHash
    pool_txn_root_hash = db_manager.get_ledger(POOL_LEDGER_ID).uncommitted_root_hash
    pool_state_root_hash = db_manager.get_state(POOL_LEDGER_ID).headHash
    alh.commit_batch(FakeSomething())

    assert alh.ledger.uncommitted_size == alh.ledger.size
    assert alh.ledger.size == size_before + 1
    check_audit_txn(txn=alh.ledger.get_last_committed_txn(),
                    view_no=3, pp_seq_no=35,
                    seq_no=initial_seq_no + 1, txn_time=11111,
                    txn_roots={
                        DOMAIN_LEDGER_ID: txn_root_hash,
                        POOL_LEDGER_ID: pool_txn_root_hash
                    },
                    state_roots={
                        DOMAIN_LEDGER_ID: state_root_hash,
                        POOL_LEDGER_ID: pool_state_root_hash
                    },
                    pool_size=initial_pool_size, domain_size=initial_domain_size + 7, config_size=initial_config_size,
                    last_pool_seqno=None,
                    last_domain_seqno=None,
                    last_config_seqno=None,
                    primaries=DEFAULT_PRIMARIES,
                    digest=digest,
                    node_reg=DEFAULT_NODE_REG)


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
    alh.commit_batch(FakeSomething())

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


def test_apply_audit_ledger_txn_new_ledger(alh, node,
                                           initial_domain_size, initial_pool_size, initial_config_size):
    check_apply_audit_txn(alh=alh,
                          txns_count=10, ledger_ids=[POOL_LEDGER_ID],
                          view_no=1, pp_sq_no=10, txn_time=10000, seq_no=2,
                          pool_size=initial_pool_size + 10, domain_size=initial_domain_size,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=1, last_config_seqno=None,
                          primaries=1,
                          node_reg=1)

    integrate_plugin_in_node(node)

    check_apply_audit_txn(alh=alh,
                          txns_count=15, ledger_ids=[DOMAIN_LEDGER_ID],
                          view_no=1, pp_sq_no=12, txn_time=10006, seq_no=3,
                          pool_size=initial_pool_size + 10, domain_size=initial_domain_size + 15,
                          config_size=initial_config_size,
                          last_pool_seqno=2, last_domain_seqno=None, last_config_seqno=None,
                          primaries=2, other_sizes={AUCTION_LEDGER_ID: 0},
                          node_reg=2)
