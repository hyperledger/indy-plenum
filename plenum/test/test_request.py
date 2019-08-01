import pytest

from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.request import Request
from plenum.common.types import OPERATION, f
from plenum.test.helper import sdk_sign_request_from_dict, sdk_multisign_request_from_dict


@pytest.fixture(params=['with_endorser', 'no_endorser'])
def endorser(request):
    if request.param == 'with_endorser':
        return '5gC6mJq5MoGPwubtU8F5Qc'
    return None


@pytest.fixture(params=['all', 'sig_only', 'sigs_only', 'no_protocol_vers',
                        'all_sdk', 'sig_only_sdk', 'sigs_only_sdk', 'no_protocol_vers_sdk',
                        'endorser'])
def req(request, looper, sdk_wallet_client, endorser):
    op = {'type': '1',
          'something': 'nothing'}
    taaa = {
        'a': 'b',
        'c': 3
    }
    if request.param.endswith('_sdk'):
        request.param = request.param[:-4]
        if request.param == 'sigs_only':
            req = sdk_multisign_request_from_dict(looper, sdk_wallet_client,
                                                  op, reqId=1513945121191691,
                                                  taa_acceptance=taaa,
                                                  endorser=endorser)
        else:
            req = sdk_sign_request_from_dict(looper, sdk_wallet_client,
                                             op, reqId=1513945121191691,
                                             taa_acceptance=taaa,
                                             endorser=endorser)

        if request.param == 'no_protocol_vers':  # TODO INDY-2072 always false here
            req.pop('protocolVersion')
        req = Request(
            req.get(f.IDENTIFIER.nm, None),
            req.get(f.REQ_ID.nm, None),
            req.get(OPERATION, None),
            req.get(f.SIG.nm, None),
            req.get(f.SIGS.nm, None),
            req.get(f.PROTOCOL_VERSION.nm, None),
            req.get(f.TAA_ACCEPTANCE.nm, None),
            req.get(f.ENDORSER.nm, None)
        )
    else:
        req = Request(operation=op, reqId=1513945121191691,
                      protocolVersion=CURRENT_PROTOCOL_VERSION, identifier="6ouriXMZkLeHsuXrN1X1fd",
                      taaAcceptance=taaa, endorser=endorser)
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

    return req


def test_request_all_identifiers_returns_empty_list_for_request_without_signatures():
    req = Request()
    assert req.all_identifiers == []


def test_as_dict(req, endorser):
    req_dct = req.as_dict
    assert req_dct.get(f.REQ_ID.nm) == req.reqId
    assert req_dct.get(OPERATION) == req.operation
    assert req_dct.get(f.IDENTIFIER.nm) == req.identifier
    assert req_dct.get(f.SIGS.nm) == req.signatures
    assert req_dct.get(f.SIG.nm) == req.signature
    assert req_dct.get(f.PROTOCOL_VERSION.nm) == req.protocolVersion
    assert req_dct.get(f.TAA_ACCEPTANCE.nm) == req.taaAcceptance
    assert req_dct.get(f.ENDORSER.nm) == req.endorser


def test_signing_payload_state(req, endorser):
    signing_state = req.signingPayloadState()
    assert signing_state.get(f.REQ_ID.nm) == req.reqId
    assert signing_state.get(OPERATION) == req.operation
    assert signing_state.get(f.IDENTIFIER.nm) == req.identifier
    assert signing_state.get(f.PROTOCOL_VERSION.nm) == req.protocolVersion
    assert signing_state.get(f.TAA_ACCEPTANCE.nm) == req.taaAcceptance
    assert signing_state.get(f.ENDORSER.nm) == req.endorser
    assert f.SIGS.nm not in signing_state
    assert f.SIG.nm not in signing_state


def test_signing_state(req, endorser):
    signing_state = req.signingState()
    assert signing_state.get(f.REQ_ID.nm) == req.reqId
    assert signing_state.get(OPERATION) == req.operation
    assert signing_state.get(f.IDENTIFIER.nm) == req.identifier
    assert signing_state.get(f.PROTOCOL_VERSION.nm) == req.protocolVersion
    assert signing_state.get(f.TAA_ACCEPTANCE.nm) == req.taaAcceptance
    assert signing_state.get(f.ENDORSER.nm) == req.endorser
    assert signing_state.get(f.SIGS.nm) == req.signatures
    assert signing_state.get(f.SIG.nm) == req.signature
