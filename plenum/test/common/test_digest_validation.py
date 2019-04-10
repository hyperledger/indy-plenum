import json

import pytest
from indy.did import create_and_store_my_did

from plenum.common.exceptions import RequestNackedException
from plenum.common.request import Request
from plenum.test.helper import sdk_gen_request, sdk_multisign_request_object, sdk_send_signed_requests, \
    sdk_get_and_check_replies, sdk_random_request_objects

from plenum.common.constants import CURRENT_PROTOCOL_VERSION, DOMAIN_LEDGER_ID

from plenum.common.util import randomString
from stp_core.loop.eventually import eventually


@pytest.fixture(scope='function')
def op(looper, sdk_wallet_stewards):
    wh, did = sdk_wallet_stewards[0]
    seed = randomString(32)
    new_steward_did, new_steward_verkey = looper.loop.run_until_complete(
        create_and_store_my_did(wh, json.dumps({'seed': seed})))
    op = {'type': '1',
          'dest': new_steward_did,
          'verkey': new_steward_verkey,
          'role': None}
    return op


def test_send_same_txn_with_different_signatures_in_separate_batches(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards, op):
    # Send two txn with same payload digest but different signatures,
    # so that they could be processed in one batch, trying to break the ledger hashes

    wh, did = sdk_wallet_stewards[0]

    req = json.dumps(sdk_gen_request(op, protocol_version=CURRENT_PROTOCOL_VERSION,
                                     identifier=did).as_dict)
    req1 = sdk_multisign_request_object(looper, sdk_wallet_stewards[0], req)
    req_obj1 = Request(**json.loads(req1))

    req2 = sdk_multisign_request_object(looper, sdk_wallet_stewards[1], req1)
    req_obj2 = Request(**json.loads(req2))

    assert req_obj1.payload_digest == req_obj2.payload_digest
    assert req_obj1.digest != req_obj2.digest

    rep1 = sdk_send_signed_requests(sdk_pool_handle, [req1])
    sdk_get_and_check_replies(looper, rep1)

    rep2 = sdk_send_signed_requests(sdk_pool_handle, [req2])
    with pytest.raises(RequestNackedException):
        sdk_get_and_check_replies(looper, rep2)


def test_send_same_txn_with_different_signatures_in_one_batch(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards, op):
    wh, did = sdk_wallet_stewards[0]

    req = json.dumps(sdk_gen_request(op, protocol_version=CURRENT_PROTOCOL_VERSION,
                                     identifier=did).as_dict)
    req1 = sdk_multisign_request_object(looper, sdk_wallet_stewards[0], req)
    req_obj1 = Request(**json.loads(req1))

    req2 = sdk_multisign_request_object(looper, sdk_wallet_stewards[1], req1)
    req_obj2 = Request(**json.loads(req2))

    assert req_obj1.payload_digest == req_obj2.payload_digest
    assert req_obj1.digest != req_obj2.digest

    lo_before = txnPoolNodeSet[0].master_replica.last_ordered_3pc[1]
    sdk_send_signed_requests(sdk_pool_handle, [req1])
    sdk_send_signed_requests(sdk_pool_handle, [req2])

    # We need to check for ordering this way, cause sdk do not allow
    # track two requests with same reqId at the same time
    def wait_one_batch(before):
        assert txnPoolNodeSet[0].master_replica.last_ordered_3pc[1] == before + 1

    looper.run(eventually(wait_one_batch, lo_before))

    pps = txnPoolNodeSet[0].master_replica.sentPrePrepares
    pp = pps[pps.keys()[-1]]
    idrs = pp.reqIdr
    assert len(idrs) == 1
    assert req_obj1.digest in idrs


def test_second_digest_is_written(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards):
    req = json.dumps(sdk_random_request_objects(1, CURRENT_PROTOCOL_VERSION, sdk_wallet_stewards[0][1])[0].as_dict)
    req = sdk_multisign_request_object(looper, sdk_wallet_stewards[0], req)
    req = sdk_multisign_request_object(looper, sdk_wallet_stewards[1], req)
    sdk_get_and_check_replies(looper, sdk_send_signed_requests(sdk_pool_handle, [req]))

    req = Request(**json.loads(req))

    ledger_id, _, full_digest = txnPoolNodeSet[0].seqNoDB.get(req.payload_digest)
    assert ledger_id == DOMAIN_LEDGER_ID
    assert full_digest == req.digest

    payload_digest = txnPoolNodeSet[0].seqNoDB.get(req.digest, full_digest=True)
    assert payload_digest == req.payload_digest
