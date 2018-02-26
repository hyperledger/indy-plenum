import pytest
from plenum.test.pool_transactions.conftest import looper
from plenum.test.helper import sdk_send_random_request, \
    sdk_send_random_requests, sdk_get_and_check_replies


def test_sdk_pool_handle(sdk_pool_handle):
    ph = sdk_pool_handle
    assert ph > 0


def test_sdk_wallet_handle(sdk_wallet_handle):
    wh = sdk_wallet_handle
    assert wh > 0


def test_sdk_trustee_wallet(sdk_wallet_trustee):
    wh, tr_did = sdk_wallet_trustee
    assert wh > 0
    assert tr_did


def test_sdk_steward_wallet(sdk_wallet_steward):
    wh, st_did = sdk_wallet_steward
    assert wh > 0
    assert st_did


def test_sdk_client_wallet(sdk_wallet_client):
    wh, cl_did = sdk_wallet_client
    assert wh > 0
    assert cl_did


def test_sdk_new_client_wallet(sdk_wallet_new_client):
    wh, cl_did = sdk_wallet_new_client
    assert wh > 0
    assert cl_did


def test_sdk_new_steward_wallet(sdk_wallet_new_steward):
    wh, cl_did = sdk_wallet_new_steward
    assert wh > 0
    assert cl_did


def test_sdk_trustee_send(looper, sdk_pool_handle, sdk_wallet_trustee):
    resp_task = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_trustee)
    _, j_resp = sdk_get_and_check_replies(looper, [resp_task])[0]
    assert j_resp['result']


def test_sdk_steward_send(looper, sdk_pool_handle, sdk_wallet_steward):
    resp_task = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_steward)
    _, j_resp = sdk_get_and_check_replies(looper, [resp_task])[0]
    assert j_resp['result']


def test_sdk_client_send(looper, sdk_pool_handle, sdk_wallet_client):
    resp_task = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)
    _, j_resp = sdk_get_and_check_replies(looper, [resp_task])[0]
    assert j_resp['result']


def test_sdk_new_client_send(looper, sdk_pool_handle, sdk_wallet_new_client):
    resp_task = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_new_client)
    _, j_resp = sdk_get_and_check_replies(looper, [resp_task])[0]
    assert j_resp['result']


def test_sdk_new_steward_send(looper, sdk_pool_handle, sdk_wallet_new_steward):
    resp_task = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_new_steward)
    _, j_resp = sdk_get_and_check_replies(looper, [resp_task])[0]
    assert j_resp['result']


def test_sdk_steward_send_many(looper, sdk_pool_handle, sdk_wallet_steward):
    resp_task = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_steward, 30)
    repl = sdk_get_and_check_replies(looper, resp_task)
    for _, resp in repl:
        assert resp['result']
