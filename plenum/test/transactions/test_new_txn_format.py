import json

import pytest

from plenum.common.constants import TXN_PAYLOAD, TXN_PAYLOAD_METADATA, TXN_PAYLOAD_METADATA_DIGEST, \
    TXN_PAYLOAD_METADATA_PAYLOAD_DIGEST, CURRENT_PROTOCOL_VERSION
from plenum.common.request import Request
from plenum.common.txn_util import transform_to_new_format, reqToTxn, get_payload_digest, get_digest
from plenum.common.types import f, OPERATION
from plenum.common.util import SortedDict
from plenum.test.helper import sdk_signed_random_requests, sdk_random_request_objects, sdk_multisign_request_object


@pytest.fixture(
    params=['all', 'sig_only', 'none_sig', 'sigs_only', 'none_sigs', 'no_protocol_vers', 'none_protocol_vers',
            'no_req_id', 'no_signatures'])
def old_and_expected(request):
    old = {
        "dest": "GEzcdDLhCpGCYRHW82kjHd",
        "verkey": "~HmUWn928bnFT6Ephf65YXv",
        "role": "101",

        "type": "1",
        "protocolVersion": 1,
        "identifier": "L5AD5g65TDQr1PPHHRoiGf",
        "reqId": 1513945121191691,
        "signature": "3SyRto3MGcBy1o4UmHoDezy1TJiNHDdU9o7TjHtYcSqgtpWzejMoHDrz3dpT93Xe8QXMF2tJVCQTtGmebmS2DkLS",
        "signatures": {"L5AD5g65TDQr1PPHHRoiGf":
                           "3SyRto3MGcBy1o4UmHoDezy1TJiNHDdU9o7TjHtYcSqgtpWzejMoHDrz3dpT93Xe8QXMF2tJVCQTtGmebmS2DkLS"},
        "txnTime": 1513945121,
        "txnId": "fea82e10e894419fe2bea7d96296a6d46f50f93f9eeda954ec461b2ed2950b62"
    }
    new_expected = SortedDict({
        "reqSignature": {
            "type": "ED25519",
            "values": [{
                "from": "L5AD5g65TDQr1PPHHRoiGf",
                "value": "3SyRto3MGcBy1o4UmHoDezy1TJiNHDdU9o7TjHtYcSqgtpWzejMoHDrz3dpT93Xe8QXMF2tJVCQTtGmebmS2DkLS"
            }]
        },
        "txn": {
            "data": {
                "dest": "GEzcdDLhCpGCYRHW82kjHd",
                "role": "101",
                "verkey": "~HmUWn928bnFT6Ephf65YXv",
            },

            "metadata": {
                "from": "L5AD5g65TDQr1PPHHRoiGf",
                "reqId": 1513945121191691,
            },

            "protocolVersion": 1,
            "type": "1",
        },
        "txnMetadata": {
            "txnTime": 1513945121,
            "seqNo": 143,
            "txnId": "fea82e10e894419fe2bea7d96296a6d46f50f93f9eeda954ec461b2ed2950b62",
        },
        "ver": "1"
    })

    if request.param == 'sig_only':
        old.pop("signatures")
    if request.param == 'none_sigs':
        old["signatures"] = None
    if request.param == 'sigs_only':
        old.pop("signature")
    if request.param == 'none_sig':
        old["signature"] = None
    if request.param == 'no_protocol_vers':
        old.pop("protocolVersion")
        new_expected["txn"].pop("protocolVersion", None)
    if request.param == 'none_protocol_vers':
        old["protocolVersion"] = None
        new_expected["txn"].pop("protocolVersion", None)
    if request.param == 'no_req_id':
        old["reqId"] = None
        new_expected["txn"]["metadata"].pop("reqId", None)
    if request.param == 'no_signatures':
        old.pop("signatures")
        old.pop("signature")
        new_expected["reqSignature"] = {}

    return old, new_expected


def test_new_txn_format(old_and_expected):
    old, new_expected = old_and_expected
    new = SortedDict(transform_to_new_format(old, 143))
    assert new == new_expected


def deserialize_req(req: str) -> Request:
    req = json.loads(req)
    return Request(identifier=req.get(f.IDENTIFIER.nm, None),
                   reqId=req.get(f.REQ_ID.nm, None),
                   operation=req.get(OPERATION, None),
                   signature=req.get(f.SIG.nm, None),
                   signatures=req.get(f.SIGS.nm, None),
                   protocolVersion=req.get(f.PROTOCOL_VERSION.nm, None))


def req_to_legacy_txn(req: Request):
    txn = reqToTxn(req)
    metadata = txn[TXN_PAYLOAD][TXN_PAYLOAD_METADATA]
    metadata[TXN_PAYLOAD_METADATA_DIGEST] = metadata[TXN_PAYLOAD_METADATA_PAYLOAD_DIGEST]
    del metadata[TXN_PAYLOAD_METADATA_PAYLOAD_DIGEST]
    return txn


def test_old_txn_metadata_digest_fallback(looper, sdk_wallet_client):
    # Create signed request and convert to legacy txn
    req_str = sdk_signed_random_requests(looper, sdk_wallet_client, 1)[0]
    req = deserialize_req(req_str)
    txn = req_to_legacy_txn(req_str)

    # Check that digests still can be extracted correctly
    assert get_payload_digest(txn) == req.payload_digest
    assert get_digest(txn) == None


def test_old_txn_metadata_multisig_digest_fallback(looper, sdk_wallet_client, sdk_wallet_client2):
    # Create signed request and convert to legacy txn
    req_str = json.dumps(sdk_random_request_objects(1, CURRENT_PROTOCOL_VERSION, sdk_wallet_client[1])[0].as_dict)
    req_str = sdk_multisign_request_object(looper, sdk_wallet_client, req_str)
    req_str = sdk_multisign_request_object(looper, sdk_wallet_client2, req_str)
    req = deserialize_req(req_str)
    txn = req_to_legacy_txn(req_str)

    # Check that digests still can be extracted correctly
    assert get_payload_digest(txn) == req.payload_digest
    assert get_digest(txn) == None
