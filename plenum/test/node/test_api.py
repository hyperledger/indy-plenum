import pytest

from common.exceptions import LogicError
from plenum.common.constants import TXN_TYPE, TXN_PAYLOAD, TXN_PAYLOAD_METADATA, TXN_PAYLOAD_METADATA_DIGEST, \
    TXN_PAYLOAD_TYPE, TXN_PAYLOAD_DATA, TXN_PAYLOAD_METADATA_REQ_ID, TXN_METADATA, TXN_METADATA_SEQ_NO, \
    TXN_PAYLOAD_METADATA_PAYLOAD_DIGEST, CURRENT_TXN_PAYLOAD_VERSIONS, TXN_VERSION, AUDIT
from plenum.common.request import Request
from plenum.test.testing_utils import FakeSomething


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
        },
        TXN_METADATA: {
            TXN_METADATA_SEQ_NO: "1"
        },
        TXN_VERSION: "1"
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
        },
        TXN_VERSION: "1"
    }

    test_node.postTxnFromCatchupAddedToLedger(2, test_txn)
    assert oldSize + 2 == test_node.seqNoDB.size


def test_send_message_without_inst_id_to_replica(test_node):
    replica = test_node.replicas[0]
    frm = "frm"
    msg = FakeSomething()
    test_node.sendToReplica(msg, frm, replica.instId)
    assert len(test_node.replicas) > 1
    for r in test_node.replicas.values():
        if r == replica:
            assert (msg, frm) in r.inBox
        else:
            assert (msg, frm) not in r.inBox


def test_send_message_to_one_replica(test_node):
    replica = test_node.replicas[0]
    frm = "frm"
    msg = FakeSomething(instId=replica.instId)
    test_node.sendToReplica(msg, frm)
    assert len(test_node.replicas) > 1
    for r in test_node.replicas.values():
        if r == replica:
            assert (msg, frm) in r.inBox
        else:
            assert (msg, frm) not in r.inBox


def test_send_message_to_incorrect_replica(test_node):
    frm = "frm"
    msg = FakeSomething(instId=100000)
    test_node.sendToReplica(msg, frm)
    assert len(test_node.replicas) > 1
    for r in test_node.replicas.values():
        assert (msg, frm) not in r.inBox


def test_send_message_for_all_without_inst_id(test_node):
    frm = "frm"
    msg = FakeSomething()
    test_node.sendToReplica(msg, frm)
    assert len(test_node.replicas) > 1
    for r in test_node.replicas.values():
        assert (msg, frm) in r.inBox