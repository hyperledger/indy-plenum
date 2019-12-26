from plenum.common.constants import NYM, NODE, CURRENT_PROTOCOL_VERSION, CURRENT_TXN_PAYLOAD_VERSIONS, \
    CURRENT_TXN_VERSION
from plenum.common.txn_util import init_empty_txn, set_payload_data, append_payload_metadata, append_txn_metadata
from plenum.common.util import SortedDict


def test_init_empty_txn_no_protocol_ver():
    txn = init_empty_txn(txn_type=NYM)
    expected = {
        "reqSignature": {},
        "txn": {
            "data": {},
            "metadata": {
            },
            "type": NYM,
            "protocolVersion": CURRENT_PROTOCOL_VERSION
        },
        "txnMetadata": {
        },
        "ver": CURRENT_TXN_VERSION
    }
    assert SortedDict(expected) == SortedDict(txn)


def test_init_empty_txn_with_protocol_ver():
    txn = init_empty_txn(txn_type=NODE, protocol_version="3")
    expected = {
        "reqSignature": {},
        "txn": {
            "data": {},
            "metadata": {
            },
            "protocolVersion": "3",
            "type": NODE,
        },
        "txnMetadata": {
        },
        "ver": CURRENT_TXN_VERSION
    }
    assert SortedDict(expected) == SortedDict(txn)


def test_set_payload_metadata():
    txn = init_empty_txn(txn_type=NODE, protocol_version="3")
    set_payload_data(txn, {"somekey": "somevalue"})
    expected = SortedDict({
        "reqSignature": {},
        "txn": {
            "data": {"somekey": "somevalue"},
            "metadata": {
            },
            "protocolVersion": "3",
            "type": NODE,
        },
        "txnMetadata": {
        },
        "ver": CURRENT_TXN_VERSION
    })
    assert SortedDict(expected) == SortedDict(txn)


def test_append_payload_metadata():
    txn = init_empty_txn(txn_type=NODE, protocol_version="3")
    set_payload_data(txn, {"somekey": "somevalue"})
    append_payload_metadata(txn, frm="DID1",
                            req_id=12345,
                            digest="random req digest",
                            payload_digest="random payload",
                            taa_acceptance={'sometaakey': "sometaavalue"})
    expected = SortedDict({
        "reqSignature": {},
        "txn": {
            "data": {"somekey": "somevalue"},
            "metadata": {
                "from": "DID1",
                "reqId": 12345,
                "digest": "random req digest",
                "payloadDigest": "random payload",
                "taaAcceptance": {'sometaakey': "sometaavalue"}
            },
            "protocolVersion": "3",
            "type": NODE,
        },
        "txnMetadata": {
        },
        "ver": CURRENT_TXN_VERSION
    })
    assert SortedDict(expected) == SortedDict(txn)


def test_append_txn_metadata():
    txn = init_empty_txn(txn_type=NODE, protocol_version="3")
    set_payload_data(txn, {"somekey": "somevalue"})
    append_payload_metadata(txn, frm="DID1", req_id=12345)
    append_txn_metadata(txn, seq_no=144, txn_time=12345678, txn_id="dddd")
    expected = SortedDict({
        "reqSignature": {},
        "txn": {
            "data": {"somekey": "somevalue"},
            "metadata": {
                "from": "DID1",
                "reqId": 12345,
            },
            "protocolVersion": "3",
            "type": NODE,
        },
        "txnMetadata": {
            "seqNo": 144,
            "txnId": "dddd",
            "txnTime": 12345678,
        },
        "ver": CURRENT_TXN_VERSION
    })
    assert SortedDict(expected) == SortedDict(txn)
