import pytest

from common.serializers.serialization import domain_state_serializer
from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID, CONFIG_LEDGER_ID
from plenum.common.txn_util import do_req_to_txn
from plenum.server.batch_handlers.audit_batch_handler import AuditBatchHandler
from plenum.test.audit_ledger.helper import check_audit_txn


@pytest.fixture(scope="module")
def db_manager(txnPoolNodeSet):
    return txnPoolNodeSet[0].db_manager


@pytest.fixture(scope="module")
def master_replica(txnPoolNodeSet):
    return txnPoolNodeSet[0].master_replica


@pytest.fixture(scope="function")
def alh(db_manager, master_replica):
    audit_ledger_handler = AuditBatchHandler(db_manager, master_replica)
    yield audit_ledger_handler
    for db in db_manager.databases.values():
        db.reset()
    set_3pc_batch(master_replica, 0, 0)


@pytest.fixture(scope="function")
def initial_pool_size(db_manager, master_replica):
    return db_manager.get_ledger(POOL_LEDGER_ID).size


@pytest.fixture(scope="function")
def initial_domain_size(db_manager, master_replica):
    return db_manager.get_ledger(DOMAIN_LEDGER_ID).size


@pytest.fixture(scope="function")
def initial_config_size(db_manager, master_replica):
    return db_manager.get_ledger(CONFIG_LEDGER_ID).size


def check_apply_audit_txn(alh,
                          master_replica,
                          txns_count, ledger_id,
                          view_no, pp_sq_no, txn_time, seq_no,
                          pool_size, domain_size, config_size,
                          last_pool_seqno, last_domain_seqno, last_config_seqno):
    db_manager = alh.database_manager
    set_3pc_batch(master_replica, view_no, pp_sq_no)
    add_txns(db_manager, ledger_id, txns_count, txn_time)
    uncommited_size_before = alh.ledger.uncommitted_size
    size_before = alh.ledger.size

    alh.post_batch_applied(ledger_id, db_manager.get_state(ledger_id).headHash, txn_time)

    assert alh.ledger.uncommitted_size == uncommited_size_before + 1
    assert size_before == alh.ledger.size

    txn = alh.ledger.get_uncommitted_txns()[-1]
    check_audit_txn(txn=txn,
                    view_no=view_no, pp_seq_no=pp_sq_no,
                    seq_no=seq_no, txn_time=txn_time,
                    ledger_id=ledger_id,
                    txn_root=db_manager.get_ledger(ledger_id).uncommittedRootHash,
                    state_root=db_manager.get_state(ledger_id).headHash,
                    pool_size=pool_size, domain_size=domain_size, config_size=config_size,
                    last_pool_seqno=last_pool_seqno,
                    last_domain_seqno=last_domain_seqno,
                    last_config_seqno=last_config_seqno)


def test_apply_audit_ledger_txn_pool_ledger(alh, master_replica,
                                            initial_domain_size, initial_pool_size, initial_config_size):
    check_apply_audit_txn(alh=alh,
                          master_replica=master_replica,
                          txns_count=10, ledger_id=POOL_LEDGER_ID,
                          view_no=1, pp_sq_no=10, txn_time=10000, seq_no=1,
                          pool_size=initial_pool_size + 10, domain_size=initial_domain_size,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=None, last_config_seqno=None)


def test_apply_audit_ledger_txn_domain_ledger(alh, master_replica,
                                              initial_domain_size, initial_pool_size, initial_config_size):
    check_apply_audit_txn(alh=alh,
                          master_replica=master_replica,
                          txns_count=15, ledger_id=DOMAIN_LEDGER_ID,
                          view_no=1, pp_sq_no=12, txn_time=10006, seq_no=1,
                          pool_size=initial_pool_size, domain_size=initial_domain_size + 15,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=None, last_config_seqno=None)


def test_apply_audit_ledger_txn_config_ledger(alh, master_replica,
                                              initial_domain_size, initial_pool_size, initial_config_size):
    check_apply_audit_txn(alh=alh,
                          master_replica=master_replica,
                          txns_count=20, ledger_id=CONFIG_LEDGER_ID,
                          view_no=1, pp_sq_no=15, txn_time=10008, seq_no=1,
                          pool_size=initial_pool_size, domain_size=initial_domain_size,
                          config_size=initial_config_size + 20,
                          last_pool_seqno=None, last_domain_seqno=None, last_config_seqno=None)


def test_apply_audit_ledger_txn_multi_ledger(alh, master_replica,
                                             initial_domain_size, initial_pool_size, initial_config_size):
    # 1. add domain txn
    check_apply_audit_txn(alh=alh,
                          master_replica=master_replica,
                          txns_count=10, ledger_id=DOMAIN_LEDGER_ID,
                          view_no=2, pp_sq_no=5, txn_time=500, seq_no=1,
                          pool_size=initial_pool_size, domain_size=initial_domain_size + 10,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=None, last_config_seqno=None)

    # 2. add pool txn
    check_apply_audit_txn(alh=alh,
                          master_replica=master_replica,
                          txns_count=6, ledger_id=POOL_LEDGER_ID,
                          view_no=2, pp_sq_no=6, txn_time=502, seq_no=2,
                          pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 10,
                          config_size=initial_config_size,
                          last_pool_seqno=None, last_domain_seqno=1, last_config_seqno=None)

    # 3. add config txn
    check_apply_audit_txn(alh=alh,
                          master_replica=master_replica,
                          txns_count=8, ledger_id=CONFIG_LEDGER_ID,
                          view_no=2, pp_sq_no=7, txn_time=502, seq_no=3,
                          pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 10,
                          config_size=initial_config_size + 8,
                          last_pool_seqno=2, last_domain_seqno=1, last_config_seqno=None)

    # 4. add domain txn
    check_apply_audit_txn(alh=alh,
                          master_replica=master_replica,
                          txns_count=2, ledger_id=DOMAIN_LEDGER_ID,
                          view_no=2, pp_sq_no=8, txn_time=550, seq_no=4,
                          pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 12,
                          config_size=initial_config_size + 8,
                          last_pool_seqno=2, last_domain_seqno=None, last_config_seqno=3)

    # 5. add domain txn
    check_apply_audit_txn(alh=alh,
                          master_replica=master_replica,
                          txns_count=7, ledger_id=DOMAIN_LEDGER_ID,
                          view_no=2, pp_sq_no=9, txn_time=551, seq_no=5,
                          pool_size=initial_pool_size + 6, domain_size=initial_domain_size + 19,
                          config_size=initial_config_size + 8,
                          last_pool_seqno=2, last_domain_seqno=None, last_config_seqno=3)

    # 6. add pool txn
    check_apply_audit_txn(alh=alh,
                          master_replica=master_replica,
                          txns_count=5, ledger_id=POOL_LEDGER_ID,
                          view_no=2, pp_sq_no=10, txn_time=551, seq_no=6,
                          pool_size=initial_pool_size + 11, domain_size=initial_domain_size + 19,
                          config_size=initial_config_size + 8,
                          last_pool_seqno=None, last_domain_seqno=5, last_config_seqno=3)


def test_reject_batch(alh):
    uncommited_size_before = alh.ledger.uncommitted_size
    size_before = alh.ledger.size

    alh.post_batch_applied(DOMAIN_LEDGER_ID, "some_state_root_1", 1000)
    alh.post_batch_applied(POOL_LEDGER_ID, "some_state_root_2", 1000)
    alh.post_batch_applied(CONFIG_LEDGER_ID, "some_state_root_3", 1000)
    alh.post_batch_applied(DOMAIN_LEDGER_ID, "some_state_root_4", 1000)
    assert alh.ledger.uncommitted_size == uncommited_size_before + 4
    assert alh.ledger.size == size_before

    alh.post_batch_rejected()
    assert alh.ledger.uncommitted_size == uncommited_size_before + 3
    assert alh.ledger.size == size_before

    alh.post_batch_rejected()
    assert alh.ledger.uncommitted_size == uncommited_size_before + 2
    assert alh.ledger.size == size_before

    alh.post_batch_rejected()
    assert alh.ledger.uncommitted_size == uncommited_size_before + 1
    assert alh.ledger.size == size_before

    alh.post_batch_rejected()
    assert alh.ledger.uncommitted_size == uncommited_size_before
    assert alh.ledger.size == size_before

    alh.post_batch_rejected()
    assert alh.ledger.uncommitted_size == uncommited_size_before
    assert alh.ledger.size == size_before


def test_commit_one_batch(alh):
    size_before = alh.ledger.size

    alh.post_batch_applied(DOMAIN_LEDGER_ID, "some_state_root_1", 11111)
    alh.commit_batch(10, "some_state_root_1", "some_txn_root_1", 11111, None)

    assert alh.ledger.uncommitted_size == alh.ledger.size
    assert alh.ledger.size == size_before + 1
    txn = alh.ledger.get_last_txn()



def test_commit_multiple_batches(alh):
    size_before = alh.ledger.size

    alh.post_batch_applied(DOMAIN_LEDGER_ID, "some_state_root_1", 11111)
    alh.post_batch_applied(POOL_LEDGER_ID, "some_state_root_2", 11112)
    alh.post_batch_applied(CONFIG_LEDGER_ID, "some_state_root_3", 11112)
    alh.post_batch_applied(DOMAIN_LEDGER_ID, "some_state_root_4", 11112)

    alh.commit_batch(10, "some_state_root_1", "some_txn_root_1", 11111, None)
    assert alh.ledger.uncommitted_size == 3 + alh.ledger.size
    assert alh.ledger.size == size_before + 1

    alh.commit_batch(5, "some_state_root_2", "some_txn_root_2", 11112, None)
    assert alh.ledger.uncommitted_size == 2 + alh.ledger.size
    assert alh.ledger.size == size_before + 2

    alh.commit_batch(5, "some_state_root_3", "some_txn_root_3", 11112, None)
    assert alh.ledger.uncommitted_size == 1 + alh.ledger.size
    assert alh.ledger.size == size_before + 3

    alh.commit_batch(5, "some_state_root_4", "some_txn_root_4", 11112, None)
    assert alh.ledger.uncommitted_size == alh.ledger.size
    assert alh.ledger.size == size_before + 4

    alh.commit_batch(15, "some_state_root_5", "some_txn_root_5", 11112, None)
    assert alh.ledger.uncommitted_size == alh.ledger.size
    assert alh.ledger.size == size_before + 4


def test_commit_and_revert_multiple_batches(alh):
    size_before = alh.ledger.size

    alh.post_batch_applied(DOMAIN_LEDGER_ID, "some_state_root_1", 11112)
    alh.post_batch_applied(POOL_LEDGER_ID, "some_state_root_2", 11113)
    alh.post_batch_rejected()
    alh.commit_batch(5, "some_state_root_1", "some_txn_root_1", 11112)
    assert alh.ledger.size == size_before + 1

    alh.post_batch_applied(CONFIG_LEDGER_ID, "some_state_root_3", 11114)
    alh.post_batch_applied(DOMAIN_LEDGER_ID, "some_state_root_4", 11115)
    alh.post_batch_rejected()
    alh.post_batch_applied(DOMAIN_LEDGER_ID, "some_state_root_5", 11116)
    alh.commit_batch(10, "some_state_root_3", "some_txn_root_3", 11114)
    assert alh.ledger.size == size_before + 2
    alh.commit_batch(5, "some_state_root_5", "some_txn_root_5", 11116)
    assert alh.ledger.size == size_before + 3


def add_txns(db_manager, ledger_id, count, txn_time):
    ledger = db_manager.get_ledger(ledger_id)
    state = db_manager.get_state(ledger_id)

    txns = [do_req_to_txn({}, {"ledger_id": ledger_id, "num": i}) for i in range(count)]
    ledger.append_txns_metadata(txns, txn_time)

    ledger.appendTxns(txns)
    for i, txn in enumerate(txns):
        state.set(bytes(ledger_id + i),
                  domain_state_serializer.serialize(txn))


def set_3pc_batch(master_replica, view_no, pp_seq_no):
    # TODO: PrePrepare needs to be passed to batch handlers explicitly
    master_replica.node.viewNo = view_no
    master_replica._lastPrePrepareSeqNo = pp_seq_no
