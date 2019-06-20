from plenum.common.constants import TXN_TYPE, TARGET_NYM, AUDIT_TXN_LEDGER_ROOT, AUDIT_TXN_STATE_ROOT, TXN_PAYLOAD, \
    TXN_PAYLOAD_DATA, TXN_METADATA, TXN_METADATA_SEQ_NO, TXN_AUTHOR_AGREEMENT_AML, AML_VERSION, ROLE
from plenum.common.ledger import Ledger
from plenum.common.transactions import PlenumTransactions
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.test.helper import sdk_gen_request


def test_audit_ledger_multiple_ledgers_in_one_batch(txnPoolNodeSet):
    # Checking first case -- first audit txn
    node = txnPoolNodeSet[0]
    audit_batch_handler = node.write_manager.audit_b_handler
    op = {
        TXN_TYPE: PlenumTransactions.NYM.value,
        TARGET_NYM: "000000000000000000000000Trustee4"
    }
    nym_req = sdk_gen_request(op, signatures={"sig1": "111"})
    node.write_manager.apply_request(nym_req, 10000)
    op2 = {TXN_TYPE: TXN_AUTHOR_AGREEMENT_AML,
           AML_VERSION: "version1"}
    pool_config_req = sdk_gen_request(op2, signatures={"sig1": "111"})
    node.write_manager.apply_request(pool_config_req, 10000)

    domain_root_hash = Ledger.hashToStr(node.domainLedger.uncommittedRootHash)
    config_root_hash = Ledger.hashToStr(node.configLedger.uncommittedRootHash)
    domain_state_root = Ledger.hashToStr(node.states[1].headHash)
    config_state_root = Ledger.hashToStr(node.states[2].headHash)

    batch = get_3PC_batch(domain_root_hash)

    txn_data = audit_batch_handler._create_audit_txn_data(batch, audit_batch_handler.ledger.get_last_txn())
    append_txn_to_ledger(txn_data, node.auditLedger, 1)

    assert txn_data[AUDIT_TXN_LEDGER_ROOT][1] == domain_root_hash
    assert txn_data[AUDIT_TXN_LEDGER_ROOT][2] == config_root_hash
    assert txn_data[AUDIT_TXN_STATE_ROOT][1] == domain_state_root
    assert txn_data[AUDIT_TXN_STATE_ROOT][2] == config_state_root

    # Checking usual case -- double update not in a first transaction
    op = {
        TXN_TYPE: PlenumTransactions.NYM.value,
        TARGET_NYM: "000000000000000000000000Trustee5"
    }
    nym_req = sdk_gen_request(op, signatures={"sig1": "111"})
    node.write_manager.apply_request(nym_req, 10000)
    op2 = {TXN_TYPE: TXN_AUTHOR_AGREEMENT_AML,
           AML_VERSION: "version2"}
    pool_config_req = sdk_gen_request(op2, signatures={"sig1": "111"})
    node.write_manager.apply_request(pool_config_req, 10000)

    # Checking second batch created
    domain_root_hash_2 = Ledger.hashToStr(node.domainLedger.uncommittedRootHash)
    config_root_hash_2 = Ledger.hashToStr(node.configLedger.uncommittedRootHash)
    domain_state_root_2 = Ledger.hashToStr(node.states[1].headHash)
    config_state_root_2 = Ledger.hashToStr(node.states[2].headHash)

    batch = get_3PC_batch(domain_root_hash_2)

    txn_data = audit_batch_handler._create_audit_txn_data(batch, audit_batch_handler.ledger.get_last_txn())

    # Checking first batch created
    assert txn_data[AUDIT_TXN_LEDGER_ROOT][1] == domain_root_hash_2
    assert txn_data[AUDIT_TXN_LEDGER_ROOT][2] == config_root_hash_2
    assert txn_data[AUDIT_TXN_STATE_ROOT][1] == domain_state_root_2
    assert txn_data[AUDIT_TXN_STATE_ROOT][2] == config_state_root_2


def test_multiple_ledgers_in_second_batch_apply_first_time(txnPoolNodeSet):
    # First txn
    node = txnPoolNodeSet[0]
    audit_batch_handler = node.write_manager.audit_b_handler
    op = {
        TXN_TYPE: PlenumTransactions.NYM.value,
        TARGET_NYM: "000000000000000000000000Trustee4",
        ROLE: None
    }
    nym_req = sdk_gen_request(op, signatures={"sig1": "111"})
    node.write_manager.apply_request(nym_req, 10000)
    op2 = {TXN_TYPE: TXN_AUTHOR_AGREEMENT_AML,
           AML_VERSION: "version2"}
    pool_config_req = sdk_gen_request(op2, signatures={"sig1": "111"})
    node.write_manager.apply_request(pool_config_req, 10000)

    domain_root_hash = Ledger.hashToStr(node.domainLedger.uncommittedRootHash)

    batch = get_3PC_batch(domain_root_hash)

    txn_data = audit_batch_handler._create_audit_txn_data(batch, audit_batch_handler.ledger.get_last_txn())
    append_txn_to_ledger(txn_data, node.auditLedger, 2)

    # Checking rare case -- batch from two ledgers, that were never audited before
    op2 = {
        TXN_TYPE: PlenumTransactions.NODE.value,
        TARGET_NYM: "000000000000000000000000Trustee1",
        ROLE: None
    }
    node_req = sdk_gen_request(op2, signatures={"sig1": "111"})
    node.write_manager.apply_request(node_req, 10000)

    op2 = {TXN_TYPE: TXN_AUTHOR_AGREEMENT_AML,
           AML_VERSION: "version2"}
    pool_config_req = sdk_gen_request(op2, signatures={"sig1": "111"})
    node.write_manager.apply_request(pool_config_req, 10000)

    pool_root_hash = Ledger.hashToStr(node.poolLedger.uncommittedRootHash)
    pool_state_root = Ledger.hashToStr(node.states[0].headHash)
    config_root_hash = Ledger.hashToStr(node.configLedger.uncommittedRootHash)
    config_state_root = Ledger.hashToStr(node.states[2].headHash)

    batch = get_3PC_batch(pool_root_hash, ledger_id=0)

    txn_data = audit_batch_handler._create_audit_txn_data(batch, audit_batch_handler.ledger.get_last_txn())

    assert txn_data[AUDIT_TXN_LEDGER_ROOT][0] == pool_root_hash
    assert txn_data[AUDIT_TXN_STATE_ROOT][0] == pool_state_root
    assert txn_data[AUDIT_TXN_LEDGER_ROOT][1] == 1
    assert 1 not in txn_data[AUDIT_TXN_STATE_ROOT].keys()
    assert txn_data[AUDIT_TXN_LEDGER_ROOT][2] == config_root_hash
    assert txn_data[AUDIT_TXN_STATE_ROOT][2] == config_state_root


def append_txn_to_ledger(txn_data, ledger, seq_no):
    txn = {
        TXN_PAYLOAD: {
            TXN_PAYLOAD_DATA: txn_data
        },
        TXN_METADATA: {
            TXN_METADATA_SEQ_NO: seq_no
        }
    }
    ledger.appendTxns([txn])


def get_3PC_batch(root_hash, ledger_id=1):
    return ThreePcBatch(ledger_id, 0, 0, 1, 5000, root_hash, "", ["Alpha", "Beta"], [])
