from indy_common.transactions import IndyTransactions
from plenum.common.constants import TXN_TYPE, DATA, CURRENT_PROTOCOL_VERSION, TARGET_NYM, AUDIT_TXN_LEDGER_ROOT
from plenum.common.ledger import Ledger
from plenum.common.transactions import PlenumTransactions
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.test.helper import sdk_gen_request

def test_audit_ledger_multiple_ledgers_in_one_batch(txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    audit_batch_handler = node.audit_handler
    domain_request_handler = node.ledger_to_req_handler[1]
    config_request_handler = node.ledger_to_req_handler[2]
    op = {
        TXN_TYPE: PlenumTransactions.NYM.value,
        TARGET_NYM: "000000000000000000000000Trustee4"
    }
    nym_req = sdk_gen_request(op, signatures={"sig1": "111"})
    domain_request_handler.apply(nym_req, 10000)
    op2 = {TXN_TYPE: IndyTransactions.POOL_CONFIG.value}
    pool_config_req = sdk_gen_request(op2, signatures={"sig1": "111"})
    config_request_handler.apply(pool_config_req, 10000)

    domain_root_hash = Ledger.hashToStr(node._domainLedger.uncommittedRootHash)
    config_root_hash = Ledger.hashToStr(node._configLedger.uncommittedRootHash)

    batch = ThreePcBatch(1, 0, 0, 1, 5000, domain_root_hash, "", ["Alpha", "Beta"], [])

    txn = audit_batch_handler._create_audit_txn_data(batch, audit_batch_handler.ledger.get_last_txn())

    assert txn[AUDIT_TXN_LEDGER_ROOT][1] == domain_root_hash
    assert txn[AUDIT_TXN_LEDGER_ROOT][2] == config_root_hash
