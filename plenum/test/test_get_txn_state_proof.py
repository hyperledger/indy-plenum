import json

import pytest
import random

from plenum.common.constants import TXN_METADATA, TXN_METADATA_SEQ_NO, OP_FIELD_NAME
from plenum.test.delayers import req_delay
from plenum.test.stasher import delay_rules
from indy.did import create_and_store_my_did
from indy.ledger import build_nym_request, build_get_txn_request, sign_and_submit_request, submit_request, build_attrib_request, build_acceptance_mechanisms_request


def nym_on_ledger(looper, sdk_pool_handle, sdk_wallet_client, sdk_wallet_steward, seed=None):
    did_future = create_and_store_my_did(sdk_wallet_client[0], json.dumps({"seed": seed}) if seed else "{}")
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


def attrib_on_ledger(looper, sdk_pool_handle, sdk_wallet_steward, sdk_client_wallet):
    attrib_req_future = build_attrib_request(sdk_wallet_steward[1], sdk_client_wallet[1], None, "{}", None)
    attrib_req = looper.loop.run_until_complete(attrib_req_future)
    attrib_resp_future = sign_and_submit_request(sdk_pool_handle, sdk_wallet_steward[0], sdk_wallet_steward[1], attrib_req)
    attrib_resp = looper.loop.run_until_complete(attrib_resp_future)
    attrib = json.loads(attrib_resp)
    print(attrib)
    assert attrib["result"]
    assert attrib["result"][TXN_METADATA]
    assert attrib["result"][TXN_METADATA][TXN_METADATA_SEQ_NO]
    return attrib["result"][TXN_METADATA][TXN_METADATA_SEQ_NO]


def aml_on_ledger(looper, sdk_pool_handle, sdk_wallet_trustee):
    ver = random.randint(1, 10000)
    aml_req_future = build_acceptance_mechanisms_request(sdk_wallet_trustee[1], "{\"test\":\"aml\"}", str(ver), None)
    aml_req = looper.loop.run_until_complete(aml_req_future)
    aml_resp_future = sign_and_submit_request(sdk_pool_handle, sdk_wallet_trustee[0], sdk_wallet_trustee[1], aml_req)
    aml_resp = looper.loop.run_until_complete(aml_resp_future)
    aml = json.loads(aml_resp)
    assert aml["result"]
    assert aml["result"][TXN_METADATA]
    assert aml["result"][TXN_METADATA][TXN_METADATA_SEQ_NO]
    return aml["result"][TXN_METADATA][TXN_METADATA_SEQ_NO]


@pytest.fixture(scope="function", params=[
    (['NYM'], 0, "DOMAIN"),
    (['NYM', 'NYM', 'NYM'], 1, "DOMAIN"),
    (['NYM', 'AML', 'NYM'], 1, "CONFIG")
])
def transactions(request, looper, sdk_pool_handle, sdk_wallet_client, sdk_wallet_steward, sdk_wallet_trustee):
    txns, for_get, id = request.param
    res = []

    for txn in txns:
        seq_no = -1
        if txn == 'NYM':
            seq_no = nym_on_ledger(looper, sdk_pool_handle, sdk_wallet_client, sdk_wallet_steward)
        elif txn == 'AML':
            seq_no = aml_on_ledger(looper, sdk_pool_handle, sdk_wallet_trustee)
        res.append(seq_no)

    return res[for_get], id


@pytest.fixture(scope='function', params=['all_responding', 'one_responding'])
def nodeSetAlwaysResponding(request, txnPoolNodeSet, transactions):
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


def test_get_txn_audit_proof(nodeSetAlwaysResponding, looper, sdk_pool_handle, transactions):
    seq_no, ledger = transactions
    response = sdk_get_txn(looper, sdk_pool_handle, seq_no, ledger)
    resp_json = json.loads(response)
    assert resp_json[OP_FIELD_NAME] == "REPLY"
