from plenum.test.pool_transactions.conftest import looper
from plenum.test.sdk.conftest import sdk_send_random_request, sdk_sign_and_submit_req, sdk_get_reply,\
    sdk_send_random_requests, sdk_get_replies, sdk_wallet_client1
from stp_core.common.log import getlogger


logger = getlogger()


def test_sdk_pool_handle(sdk_pool_handle):
    ph = sdk_pool_handle
    assert ph > 0


def test_sdk_wallet_handle(sdk_wallet_handle):
    wh = sdk_wallet_handle
    assert wh > 0


def test_sdk_steward_wallet(sdk_wallet_steward):
    wh, st_did = sdk_wallet_steward
    assert wh > 0
    assert st_did


def test_sdk_client1_wallet(sdk_wallet_client1):
    wh, cl_did = sdk_wallet_client1
    assert wh > 0
    assert cl_did


def test_sdk_steward_send(looper, sdk_pool_handle, sdk_wallet_steward):
    resp_task = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_steward)
    _, j_resp = sdk_get_reply(looper, resp_task)
    assert j_resp['result']


def test_sdk_client_send(looper, sdk_pool_handle, sdk_wallet_client1):
    resp_task = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client1)
    _, j_resp = sdk_get_reply(looper, resp_task)
    assert j_resp['result']


def test_sdk_steward_send_many(looper, sdk_pool_handle, sdk_wallet_steward):
    resp_task = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_steward, 30)
    repl = sdk_get_replies(looper, resp_task)
    for _, resp in repl:
        assert resp['result']
