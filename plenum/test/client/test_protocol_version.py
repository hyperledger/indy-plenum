import pytest
from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.exceptions import RequestNackedException
from plenum.test.helper import sdk_send_signed_requests, \
    sdk_get_and_check_replies, sdk_random_request_objects, sdk_sign_request_objects


@pytest.yield_fixture(scope="function", params=['1', '2'])
def request_num(request):
    return int(request.param)


def test_request_no_protocol_version(looper, txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_client,
                                     request_num):
    _, did = sdk_wallet_client
    reqs_obj = sdk_random_request_objects(request_num, identifier=did,
                                          protocol_version=None)
    for req_obj in reqs_obj:
        assert req_obj.protocolVersion == None

    signed_reqs = sdk_sign_request_objects(looper, sdk_wallet_client, reqs_obj)
    reqs = sdk_send_signed_requests(sdk_pool_handle, signed_reqs)
    sdk_get_and_check_replies(looper, reqs)


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
    with pytest.raises(RequestNackedException)as e:
        sdk_get_and_check_replies(looper, reqs)
    assert 'Unknown protocol version value -1' in e._excinfo[1].args[0]
