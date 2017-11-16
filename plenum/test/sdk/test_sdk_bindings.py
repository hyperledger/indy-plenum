from plenum.test.pool_transactions.conftest import looper
from plenum.test.sdk.conftest import sdk_sign_and_submit_req, sdk_random_op
from indy.error import ErrorCode
from indy.ledger import build_get_nym_request, submit_request
import json


def test_sdk_pool_handle(txnPoolNodeSet, sdk_pool_handle):
    ph = sdk_pool_handle
    assert ph > 0


def test_sdk_wallet_handle(sdk_wallet_handle):
    wh = sdk_wallet_handle
    assert wh > 0


def test_sdk_trustee1_wallet(sdk_wallet_trustee1):
    wh, tr_did = sdk_wallet_trustee1
    assert wh > 0
    assert tr_did


def test_sdk_trustee_send(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee1):
    op = sdk_random_op()
    trustee_wh, trustee_did = sdk_wallet_trustee1
    resp = sdk_sign_and_submit_req(looper, sdk_pool_handle, trustee_wh, trustee_did, op)
    assert resp == ErrorCode.LedgerInvalidTransaction


def test_sdk_trustee_get_nym(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee1):
    trustee_wh, trustee_did = sdk_wallet_trustee1
    get_nym_txn_req = looper.run(build_get_nym_request(trustee_did, trustee_did))
    get_nym_txn_resp = looper.run(submit_request(sdk_pool_handle, get_nym_txn_req))
    #get_nym_txn_resp = sdk_sign_and_submit_req(looper, sdk_pool_handle, trustee_wh, trustee_did, get_nym_txn_req)

    assert get_nym_txn_resp

    get_nym_txn_resp = json.loads(get_nym_txn_resp)

    assert get_nym_txn_resp['result']['dest'] == trustee_did
