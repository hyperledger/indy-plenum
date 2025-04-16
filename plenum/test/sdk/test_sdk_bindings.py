from plenum.test.helper import vdr_send_random_request, \
    vdr_send_random_requests, vdr_get_and_check_replies, vdr_send_random_and_check
from plenum.test.pool_transactions.helper import vdr_pool_refresh


def test_sdk_pool_handle(vdr_pool_handle):
    ph = vdr_pool_handle
    assert ph > 0


def test_sdk_wallet_handle(vdr_wallet_handle):
    wh = vdr_wallet_handle
    assert wh > 0


def test_sdk_trustee_wallet(vdr_wallet_trustee):
    wh, tr_did = vdr_wallet_trustee
    assert wh > 0
    assert tr_did


def test_sdk_steward_wallet(vdr_wallet_steward):
    wh, st_did = vdr_wallet_steward
    assert wh > 0
    assert st_did


def test_sdk_client_wallet(vdr_wallet_client):
    wh, cl_did = vdr_wallet_client
    assert wh > 0
    assert cl_did


def test_sdk_new_client_wallet(vdr_wallet_new_client):
    wh, cl_did = vdr_wallet_new_client
    assert wh > 0
    assert cl_did


def test_sdk_new_steward_wallet(vdr_wallet_new_steward):
    wh, cl_did = vdr_wallet_new_steward
    assert wh > 0
    assert cl_did


def test_sdk_trustee_send(looper, vdr_pool_handle, vdr_wallet_trustee):
    resp_task = vdr_send_random_request(looper, vdr_pool_handle, vdr_wallet_trustee)
    _, j_resp = vdr_get_and_check_replies(looper, [resp_task])[0]
    assert j_resp['result']


def test_sdk_steward_send(looper, vdr_pool_handle, vdr_wallet_steward):
    resp_task = vdr_send_random_request(looper, vdr_pool_handle, vdr_wallet_steward)
    _, j_resp = vdr_get_and_check_replies(looper, [resp_task])[0]
    assert j_resp['result']


def test_sdk_client_send(looper, vdr_pool_handle, vdr_wallet_client):
    resp_task = vdr_send_random_request(looper, vdr_pool_handle, vdr_wallet_client)
    _, j_resp = vdr_get_and_check_replies(looper, [resp_task])[0]
    assert j_resp['result']


def test_sdk_client2_send(looper, vdr_pool_handle, vdr_wallet_client2):
    resp_task = vdr_send_random_request(looper, vdr_pool_handle, vdr_wallet_client2)
    _, j_resp = vdr_get_and_check_replies(looper, [resp_task])[0]
    assert j_resp['result']


def test_sdk_new_client_send(looper, vdr_pool_handle, vdr_wallet_new_client):
    resp_task = vdr_send_random_request(looper, vdr_pool_handle, vdr_wallet_new_client)
    _, j_resp = vdr_get_and_check_replies(looper, [resp_task])[0]
    assert j_resp['result']


def test_sdk_new_steward_send(looper, vdr_pool_handle, vdr_wallet_new_steward):
    resp_task = vdr_send_random_request(looper, vdr_pool_handle, vdr_wallet_new_steward)
    _, j_resp = vdr_get_and_check_replies(looper, [resp_task])[0]
    assert j_resp['result']


def test_sdk_steward_send_many(looper, vdr_pool_handle, vdr_wallet_steward):
    resp_task = vdr_send_random_requests(looper, vdr_pool_handle, vdr_wallet_steward, 30)
    repl = vdr_get_and_check_replies(looper, resp_task)
    for _, resp in repl:
        assert resp['result']


def test_sdk_pool_refresh(looper, txnPoolNodeSet, vdr_pool_handle, vdr_wallet_client):
    vdr_pool_refresh(looper, vdr_pool_handle)
    vdr_send_random_and_check(looper, txnPoolNodeSet, vdr_pool_handle,
                              vdr_wallet_client, 1)
