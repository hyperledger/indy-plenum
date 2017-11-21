from plenum.test.pool_transactions.conftest import looper
from plenum.test.sdk.conftest import sdk_send_random_request, sdk_sign_and_submit_req
from indy.ledger import build_get_nym_request
import json
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


def test_sdk_steward_send(looper, sdk_pool_handle, sdk_wallet_steward):
    steward_wh, steward_did = sdk_wallet_steward
    resp_task = sdk_send_random_request(looper, sdk_pool_handle, steward_wh, steward_did)

    resp = looper.run(resp_task)

    j_resp = json.loads(resp)
    assert j_resp


def test_sdk_steward_get_nym(looper, sdk_pool_handle, sdk_wallet_steward):
    steward_wh, steward_did = sdk_wallet_steward
    get_nym_txn_req = looper.run(build_get_nym_request(steward_did, steward_did))
    get_nym_txn_task = sdk_sign_and_submit_req(sdk_pool_handle, steward_wh, steward_did, get_nym_txn_req)

    get_nym_txn_resp = looper.run(get_nym_txn_task)

    assert get_nym_txn_resp

    get_nym_txn_resp = json.loads(get_nym_txn_resp)
    assert get_nym_txn_resp['result']['dest'] == steward_did
