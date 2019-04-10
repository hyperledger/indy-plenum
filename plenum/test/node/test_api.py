import pytest

from common.exceptions import LogicError
from plenum.common.constants import TXN_TYPE, TXN_PAYLOAD, TXN_PAYLOAD_METADATA, TXN_PAYLOAD_METADATA_DIGEST, \
    TXN_PAYLOAD_TYPE, TXN_PAYLOAD_DATA, TXN_PAYLOAD_METADATA_REQ_ID, TXN_METADATA, TXN_METADATA_SEQ_NO, \
    TXN_PAYLOAD_METADATA_PAYLOAD_DIGEST
from plenum.common.request import Request


def test_on_view_change_complete_fails(test_node):
    with pytest.raises(LogicError) as excinfo:
       test_node.on_view_change_complete()
    assert "Not all replicas have primaries" in str(excinfo.value)


def test_ledger_id_for_request_fails(test_node):
    for r in (Request(operation={}), Request(operation={TXN_TYPE: None})):
        with pytest.raises(ValueError) as excinfo:
           test_node.ledger_id_for_request(r)
        assert "TXN_TYPE is not defined for request" in str(excinfo.value)


def test_seq_no_db_updates(test_node):
    oldSize = test_node.seqNoDB.size
    test_txn = {
        TXN_PAYLOAD: {
            TXN_PAYLOAD_TYPE: "2",
            TXN_PAYLOAD_METADATA: {
                TXN_PAYLOAD_METADATA_DIGEST: "11222",
                TXN_PAYLOAD_METADATA_PAYLOAD_DIGEST: "112222",
            },
            TXN_PAYLOAD_DATA: {}
        }
    }

    test_node.postTxnFromCatchupAddedToLedger(2, test_txn, False)
    assert oldSize == test_node.seqNoDB.size


def test_seq_no_db_updates_by_default(test_node):
    oldSize = test_node.seqNoDB.size
    test_txn = {
        TXN_PAYLOAD: {
            TXN_PAYLOAD_TYPE: "2",
            TXN_PAYLOAD_METADATA: {
                TXN_PAYLOAD_METADATA_DIGEST: "11222",
                TXN_PAYLOAD_METADATA_PAYLOAD_DIGEST: "112222",
                TXN_PAYLOAD_METADATA_REQ_ID: "12"
            },
            TXN_PAYLOAD_DATA: {}
        },
        TXN_METADATA: {
            TXN_METADATA_SEQ_NO: "1"
        }
    }

    test_node.postTxnFromCatchupAddedToLedger(2, test_txn)
    assert oldSize + 2 == test_node.seqNoDB.size
