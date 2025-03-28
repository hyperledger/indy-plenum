import pytest
import json
from indy_vdr.error import VdrError
from plenum.common.messages.node_messages import LedgerStatus
from plenum.common.types import f
from plenum.server.node import Node
from plenum.common.request import Request
from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.exceptions import RequestNackedException, CommonSdkIOException
from plenum.test.helper import vdr_send_signed_requests, \
    vdr_get_and_check_replies, vdr_random_request_objects, \
    vdr_sign_request_objects, vdr_get_bad_response, vdr_send_random_and_check, vdr_signed_random_requests

error_msg = 'Make sure that the latest LibIndy is used ' \
            'and `set_protocol_version({})` is called' \
    .format(CURRENT_PROTOCOL_VERSION)


@pytest.fixture(scope="function", params=['1', '2'])
def request_num(request):
    return int(request.param)


def test_client_send_incorrect_ledger_status(looper, txnPoolNodeSet):
    # Client send LEDGER_STATUS without protocoloVersion field.
    # Node send REQNACK and client's LEDGER_STATUS back
    spy = txnPoolNodeSet[0].spylog

    # msg without protocolVersion field
    msg = {'op': 'LEDGER_STATUS', f.LEDGER_ID.nm: 0, f.TXN_SEQ_NO.nm: 4,
           f.PP_SEQ_NO.nm: None, f.VIEW_NO.nm: None,
           f.MERKLE_ROOT.nm: '31ftJ1dwFmtVhdPqKo55BYX9bYxezNRioxMvPoZTjKZ5'}
    sender = 'client_1'
    wrappedMsg = (msg, sender)

    # emulate client sends LEDGER_STATUS
    txnPoolNodeSet[0].handleOneClientMsg(wrappedMsg)

    # node discarded message
    discards = spy.getAll(Node.discard)
    flag = False
    for discrad in discards:
        if discrad.params['msg'][0] == msg:
            flag = True
            assert 'missed fields - protocolVersion. ' + \
                   error_msg in discrad.params['reason']
    assert flag

    # node sent LEDGER_STATUS back to this client
    sends = spy.getAll(Node.transmitToClient)
    assert len([send for send in sends if
                send.params['remoteName'] == sender and
                isinstance(send.params['msg'], LedgerStatus)]) == 1


def test_client_send_correct_ledger_status(looper,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           txnPoolNodeSet):
    # Client send LEDGER_STATUS with protocoloVersion field.
    # Node send her LEDGER_STATUS back
    vdr_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client, 1)

    # node sent LEDGER_STATUS
    spy = txnPoolNodeSet[0].spylog
    sends = spy.getAll(Node.transmitToClient)
    assert len([send for send in sends if
                send.params['remoteName'] != 'client_1' and
                isinstance(send.params['msg'], LedgerStatus)]) == 1

#Proposed Test need confirmation
def test_request_none_protocol_version(looper, txnPoolNodeSet,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       request_num):
    _, did = sdk_wallet_client
    req_obj = Request(identifier=did, protocolVersion=None)
    assert req_obj.protocolVersion == None

    with pytest.raises(VdrError) as e:
        vdr_signed_random_requests(looper, sdk_wallet_client, request_num, protocol_version=None)
    #sdk_get_bad_response(looper, reqs, RequestNackedException,
    #                     'missed fields - protocolVersion. ' + error_msg)


def test_request_with_outdated_version(looper,
                                       txnPoolNodeSet,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       request_num):
    _, did = sdk_wallet_client
    protocol = int(CURRENT_PROTOCOL_VERSION) - 1
    req_obj = Request(identifier=did, protocolVersion=protocol)
    assert req_obj.protocolVersion == protocol

    with pytest.raises(VdrError) as e:
        vdr_signed_random_requests(looper, sdk_wallet_client, request_num, protocol_version=CURRENT_PROTOCOL_VERSION - 1)


def test_request_with_invalid_version(looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_client,
                                      request_num):
    _, did = sdk_wallet_client
    req_obj = Request(identifier=did, protocolVersion=-1)
    assert req_obj.protocolVersion == -1

    signed_objects = vdr_signed_random_requests(looper, sdk_wallet_client, request_num)
    reqs = vdr_send_signed_requests(sdk_pool_handle, signed_objects, looper)
    vdr_get_bad_response(looper, reqs, RequestNackedException,
                         'missed fields - protocolVersion. ' + error_msg)
    
    


def test_request_with_correct_version(looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_client,
                                      request_num):
    _, did = sdk_wallet_client
    reqs_obj = vdr_random_request_objects(request_num, identifier=did,
                                          protocol_version=CURRENT_PROTOCOL_VERSION)
    for req_obj in reqs_obj:
        assert json.loads(req_obj.body)["protocolVersion"] == CURRENT_PROTOCOL_VERSION

    signed_reqs = vdr_sign_request_objects(looper, sdk_wallet_client, reqs_obj)
    reqs = vdr_send_signed_requests(sdk_pool_handle, signed_reqs, looper)
    vdr_get_and_check_replies(looper, reqs)
