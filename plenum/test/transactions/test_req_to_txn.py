import pytest

from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, append_txn_metadata
from plenum.common.types import OPERATION, f
from plenum.common.util import SortedDict
from plenum.test.helper import sdk_sign_request_from_dict


@pytest.fixture(params=['all', 'sig_only', 'sigs_only', 'no_protocol_vers',
                        'all_sdk', 'sig_only_sdk', 'sigs_only_sdk', 'no_protocol_vers_sdk'])
def req_and_expected(request, looper, sdk_wallet_client):
    op = {'type': '1',
          'something': 'nothing'}
    taaa = {
        'a': 'b',
        'c': 3
    }
    if request.param.endswith('_sdk'):
        req = sdk_sign_request_from_dict(looper, sdk_wallet_client,
                                         op, reqId=1513945121191691,
                                         taa_acceptance=taaa)
        request.param = request.param[:-4]
        # TODO: support multi-sig in SDK
        # if request.param == 'sig_only':
        #     req.pop('signatures')
        # if request.param == 'sigs_only':
        #     req.pop('signature')
        if request.param == 'no_protocol_vers':  # TODO INDY-2072 always false here
            req.pop('protocolVersion')
        r = Request(
            req.get(f.IDENTIFIER.nm, None),
            req.get(f.REQ_ID.nm, None),
            req.get(OPERATION, None),
            req.get(f.SIG.nm, None),
            req.get(f.SIGS.nm, None),
            req.get(f.PROTOCOL_VERSION.nm, None),
            req.get(f.TAA_ACCEPTANCE.nm, None)
        )
        digest = r.digest
        payload_digest = r.payload_digest
        sign = req.get(f.SIG.nm)
    else:
        req = Request(operation=op, reqId=1513945121191691,
                      protocolVersion=CURRENT_PROTOCOL_VERSION, identifier="6ouriXMZkLeHsuXrN1X1fd",
                      taaAcceptance=taaa)
        sign = "2DaRm3nt6H5fJu2TP5vxqbaDCtABPYmUTSX4ocnY8fVGgyJMVNaeh2z6JZhcW1gbmGKJcZopZMKZJwADuXFFJobM"
        req.signature = sign
        req.add_signature("6ouriXMZkLeHsuXrN1X1fd",
                          sign)
        if request.param == 'sig_only':
            req.signatures = None
        if request.param == 'sigs_only':
            req.signature = None
        if request.param == 'no_protocol_vers':
            req.protocolVersion = None
        digest = req.digest
        payload_digest = req.payload_digest

    new_expected = SortedDict({
        "reqSignature": {
            "type": "ED25519",
            "values": [{
                "from": "6ouriXMZkLeHsuXrN1X1fd",
                "value": sign
            }]
        },
        "txn": {
            "data": {
                "something": "nothing",
            },

            "metadata": {
                "from": "6ouriXMZkLeHsuXrN1X1fd",
                "reqId": 1513945121191691,
                "taaAcceptance": {
                    "a": "b",
                    "c": 3
                }
            },

            "protocolVersion": CURRENT_PROTOCOL_VERSION,
            "type": "1",
        },
        "txnMetadata": {
            "txnTime": 1513945121,
        },
        "ver": "1"

    })

    if request.param == 'no_protocol_vers':
        new_expected["txn"].pop("protocolVersion", None)
    if digest is not None:
        new_expected["txn"]["metadata"]["digest"] = digest
    if payload_digest is not None:
        new_expected["txn"]["metadata"]["payloadDigest"] = payload_digest

    return req, new_expected


def test_req_to_txn(req_and_expected):
    req, new_expected = req_and_expected
    txn = append_txn_metadata(reqToTxn(req), txn_time=1513945121)
    new = SortedDict(txn)
    assert new == new_expected


def test_req_to_txn_with_seq_no(req_and_expected):
    req, new_expected = req_and_expected
    new = SortedDict(
        append_txn_metadata(
            reqToTxn(req),
            seq_no=143,
            txn_time=2613945121)
    )
    new_expected["txnMetadata"]["txnTime"] = 2613945121
    new_expected["txnMetadata"]["seqNo"] = 143
    assert new == new_expected
