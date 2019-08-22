import json

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, TXN_METADATA, TXN_METADATA_SEQ_NO, OP_FIELD_NAME
from plenum.test.delayers import req_delay
from plenum.test.stasher import delay_rules
from indy.did import create_and_store_my_did
from indy.ledger import build_nym_request, build_get_txn_request, sign_and_submit_request, submit_request


@pytest.fixture(scope="module")
def nym_on_ledger(looper, sdk_pool_handle, sdk_wallet_client, sdk_wallet_steward):
    did_future = create_and_store_my_did(sdk_wallet_client[0], '{"seed": "00000000000000000000000011111111"}')
    did, vk = looper.loop.run_until_complete(did_future)
    nym_req_future = build_nym_request(sdk_wallet_steward[1], did, vk, None, None)
    nym_req = looper.loop.run_until_complete(nym_req_future)
    nym_resp_future = sign_and_submit_request(sdk_pool_handle, sdk_wallet_steward[0], sdk_wallet_steward[1], nym_req)
    nym_resp = looper.loop.run_until_complete(nym_resp_future)
    nym = json.loads(nym_resp)
    assert nym["result"]
    assert nym["result"][TXN_METADATA]
    assert nym["result"][TXN_METADATA][TXN_METADATA_SEQ_NO]
    return nym["result"][TXN_METADATA][TXN_METADATA_SEQ_NO]


@pytest.fixture(scope='function', params=['all_responding', 'one_responding'])
def nodeSetAlwaysResponding(request, txnPoolNodeSet, nym_on_ledger):
    if request.param == 'all_responding':
        yield txnPoolNodeSet
    else:
        stashers = [node.clientIbStasher for node in txnPoolNodeSet[1:]]
        with delay_rules(stashers, req_delay()):
            yield txnPoolNodeSet


def sdk_get_txn(looper, sdk_pool_handle, seq_no, ledger_id):
    get_txn_request_future = build_get_txn_request(None, ledger_id, seq_no)
    get_txn_request = looper.loop.run_until_complete(get_txn_request_future)
    get_txn_response_future = submit_request(sdk_pool_handle, get_txn_request)
    get_txn_response = looper.loop.run_until_complete(get_txn_response_future)
    return get_txn_response


def test_get_txn_audit_proof(nodeSetAlwaysResponding, looper, sdk_pool_handle, nym_on_ledger):
    response = sdk_get_txn(looper, sdk_pool_handle, nym_on_ledger, "DOMAIN")
    resp_json = json.loads(response)
    assert resp_json[OP_FIELD_NAME] == "REPLY"
