from typing import Optional

from plenum.common.constants import DOMAIN_LEDGER_ID, CURRENT_PROTOCOL_VERSION
from plenum.common.txn_util import reqToTxn
from plenum.test.helper import sdk_random_request_objects


def test_post_genesis_txn_from_catchup_added_to_ledger(looper, txnPoolNodeSet):
    node = txnPoolNodeSet[0]

    def add_txn_to_ledger(txn_time: Optional[int]) -> dict:
        nonlocal node
        req = sdk_random_request_objects(1, CURRENT_PROTOCOL_VERSION, identifier='someidentifier')[0]
        txn = reqToTxn(req)
        node.domainLedger.append_txns_metadata([txn], txn_time=txn_time)
        node.domainLedger.appendTxns([txn])
        return txn

    # Process some genesis txn (which doesn't have timestamp)
    genesis_txn = add_txn_to_ledger(txn_time=None)
    node.postTxnFromCatchupAddedToLedger(DOMAIN_LEDGER_ID, genesis_txn)

    # Process some other txn (which does have timestamp)
    other_txn = add_txn_to_ledger(txn_time=13439852)
    node.postTxnFromCatchupAddedToLedger(DOMAIN_LEDGER_ID, other_txn)
