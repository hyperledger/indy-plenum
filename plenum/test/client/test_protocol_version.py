import pytest
from plenum.server.node import Node

from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.exceptions import RequestNackedException
from plenum.test.helper import sdk_send_signed_requests, \
    sdk_get_and_check_replies, sdk_random_request_objects, \
    sdk_sign_request_objects, sdk_get_bad_response, sdk_send_random_and_check

error_msg = 'Please update libindy or indy-node to the latest stable version'


@pytest.yield_fixture(scope="function", params=['1', '2'])
def request_num(request):
    return int(request.param)


def test_client_send_incorrect_ledger_status(looper, txnPoolNodeSet,
                                             sdk_pool_handle,
                                             sdk_wallet_client):
    # SDK client send LEDGER_STATUS without protocoloVersion.
    # Node discard this message and send client's LEDGER_STATUS back
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)
    spy = txnPoolNodeSet[0].spylog

    discards = spy.getAll(Node.discard)
    output = discards[0][3]
    assert output['msg'][0]['op'] == 'LEDGER_STATUS'
    assert 'missed fields - protocolVersion. ' + error_msg in output['reason']

    sends = spy.getAll(Node.send_ledger_status_to_client)
    assert len(sends) == 1


def test_request_none_protocol_version(looper, txnPoolNodeSet,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       request_num):
    _, did = sdk_wallet_client
    req_objs = sdk_random_request_objects(request_num, identifier=did,
                                          protocol_version=None)
    for req_obj in req_objs:
        assert req_obj.protocolVersion == None

    signed_reqs = sdk_sign_request_objects(looper, sdk_wallet_client, req_objs)
    reqs = sdk_send_signed_requests(sdk_pool_handle, signed_reqs)
    sdk_get_bad_response(looper, reqs, RequestNackedException,
                         'missed fields - protocolVersion. ' + error_msg)


def test_request_with_outdated_version(looper,
                                       txnPoolNodeSet,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       request_num):
    _, did = sdk_wallet_client
    reqs_obj = sdk_random_request_objects(request_num, identifier=did,
                                          protocol_version=CURRENT_PROTOCOL_VERSION - 1)
    for req_obj in reqs_obj:
        assert req_obj.protocolVersion == CURRENT_PROTOCOL_VERSION - 1

    signed_reqs = sdk_sign_request_objects(looper, sdk_wallet_client, reqs_obj)
    reqs = sdk_send_signed_requests(sdk_pool_handle, signed_reqs)
    sdk_get_bad_response(looper, reqs, RequestNackedException,
                         'differs from current protocol version ({}). '.format(CURRENT_PROTOCOL_VERSION) + error_msg)


def test_request_with_invalid_version(looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_client,
                                      request_num):
    _, did = sdk_wallet_client
    reqs_obj = sdk_random_request_objects(request_num, identifier=did,
                                          protocol_version=-1)
    for req_obj in reqs_obj:
        assert req_obj.protocolVersion == -1

    signed_reqs = sdk_sign_request_objects(looper, sdk_wallet_client, reqs_obj)
    reqs = sdk_send_signed_requests(sdk_pool_handle, signed_reqs)
    sdk_get_bad_response(looper, reqs, RequestNackedException,
                         'Unknown protocol version value (-1). ' + error_msg)


def test_request_with_correct_version(looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_client,
                                      request_num):
    _, did = sdk_wallet_client
    reqs_obj = sdk_random_request_objects(request_num, identifier=did,
                                          protocol_version=CURRENT_PROTOCOL_VERSION)
    for req_obj in reqs_obj:
        assert req_obj.protocolVersion == CURRENT_PROTOCOL_VERSION

    signed_reqs = sdk_sign_request_objects(looper, sdk_wallet_client, reqs_obj)
    reqs = sdk_send_signed_requests(sdk_pool_handle, signed_reqs)
    sdk_get_and_check_replies(looper, reqs)
