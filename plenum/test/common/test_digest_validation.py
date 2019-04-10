import json
import types

import pytest
from indy.did import create_and_store_my_did

from plenum.test.node_request.helper import sdk_ensure_pool_functional

from plenum.server.node import Node
from plenum.test.test_node import getPrimaryReplica

from plenum.common.exceptions import RequestNackedException, SuspiciousPrePrepare
from plenum.common.request import Request
from plenum.test.helper import sdk_gen_request, sdk_multisign_request_object, sdk_send_signed_requests, \
    sdk_get_and_check_replies, sdk_random_request_objects, waitForViewChange, max_3pc_batch_limits

from plenum.common.constants import CURRENT_PROTOCOL_VERSION, DOMAIN_LEDGER_ID, NodeHooks, TXN_TYPE

from plenum.common.util import randomString
from plenum.test.testing_utils import FakeSomething
from stp_core.loop.eventually import eventually


@pytest.fixture(scope='function')
def op(looper, sdk_wallet_stewards):
    wh, did = sdk_wallet_stewards[0]
    seed = randomString(32)
    new_did, new_verkey = looper.loop.run_until_complete(
        create_and_store_my_did(wh, json.dumps({'seed': seed})))
    op = {'type': '1',
          'dest': new_did,
          'verkey': new_verkey,
          'role': None}
    return op


@pytest.fixture(scope='function')
def two_requests(looper, op, sdk_wallet_stewards):
    wh, did = sdk_wallet_stewards[0]

    req = json.dumps(sdk_gen_request(op, protocol_version=CURRENT_PROTOCOL_VERSION,
                                     identifier=did).as_dict)
    req1 = sdk_multisign_request_object(looper, sdk_wallet_stewards[0], req)
    req_obj1 = Request(**json.loads(req1))

    req2 = sdk_multisign_request_object(looper, sdk_wallet_stewards[1], req1)
    req_obj2 = Request(**json.loads(req2))

    assert req_obj1.payload_digest == req_obj2.payload_digest
    assert req_obj1.digest != req_obj2.digest
    return req1, req2


def test_send_same_txn_with_different_signatures_in_separate_batches(
        looper, txnPoolNodeSet, sdk_pool_handle, two_requests):
    # Send two txn with same payload digest but different signatures,
    # so that they could be processed in one batch, trying to break the ledger hashes

    req1, req2 = two_requests

    rep1 = sdk_send_signed_requests(sdk_pool_handle, [req1])
    sdk_get_and_check_replies(looper, rep1)

    rep2 = sdk_send_signed_requests(sdk_pool_handle, [req2])
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, rep2)
    e.match('Same txn was already ordered with different signatures')


def test_send_same_txn_with_different_signatures_in_one_batch(
        looper, txnPoolNodeSet, sdk_pool_handle, two_requests, tconf):
    req1, req2 = two_requests

    lo_before = txnPoolNodeSet[0].master_replica.last_ordered_3pc[1]

    old_reqs = len(txnPoolNodeSet[0].requests)
    with max_3pc_batch_limits(tconf, size=2):
        sdk_send_signed_requests(sdk_pool_handle, [req1])
        sdk_send_signed_requests(sdk_pool_handle, [req2])

        # We need to check for ordering this way, cause sdk do not allow
        # track two requests with same reqId at the same time
        def wait_one_batch(before):
            assert txnPoolNodeSet[0].master_replica.last_ordered_3pc[1] == before + 1

        looper.run(eventually(wait_one_batch, lo_before))

    assert len(txnPoolNodeSet[0].requests) == old_reqs + 2

    pps = txnPoolNodeSet[0].master_replica.sentPrePrepares
    pp = pps[pps.keys()[-1]]
    idrs = pp.reqIdr
    assert len(idrs) == 1
    assert Request(**json.loads(req1)).digest in idrs


def test_suspicious_primary_send_same_request_with_different_signatures(
        looper, txnPoolNodeSet, sdk_pool_handle, two_requests):
    def malicious_dynamic_validation(self, request: Request):
        self.execute_hook(NodeHooks.PRE_DYNAMIC_VALIDATION, request=request)

        operation = request.operation
        req_handler = self.get_req_handler(txn_type=operation[TXN_TYPE])
        req_handler.validate(request)

        self.execute_hook(NodeHooks.POST_DYNAMIC_VALIDATION, request=request)

    assert txnPoolNodeSet[0].master_replica.isPrimary
    txnPoolNodeSet[0].doDynamicValidation = types.MethodType(malicious_dynamic_validation, txnPoolNodeSet[0])

    req1, req2 = two_requests

    old_view = txnPoolNodeSet[0].viewNo
    sdk_send_signed_requests(sdk_pool_handle, [req1])
    sdk_send_signed_requests(sdk_pool_handle, [req2])

    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=old_view + 1)
    txnPoolNodeSet[0].doDynamicValidation = types.MethodType(Node.doDynamicValidation, txnPoolNodeSet[0])


def test_second_digest_is_written(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards):
    req = json.dumps(sdk_random_request_objects(1, CURRENT_PROTOCOL_VERSION, sdk_wallet_stewards[0][1])[0].as_dict)
    req = sdk_multisign_request_object(looper, sdk_wallet_stewards[0], req)
    req = sdk_multisign_request_object(looper, sdk_wallet_stewards[1], req)
    sdk_get_and_check_replies(looper, sdk_send_signed_requests(sdk_pool_handle, [req]))

    req = Request(**json.loads(req))

    ledger_id, _ = txnPoolNodeSet[0].seqNoDB.get_by_payload_digest(req.payload_digest)
    assert ledger_id == DOMAIN_LEDGER_ID

    payload_digest = txnPoolNodeSet[0].seqNoDB.get_by_full_digest(req.digest)
    assert payload_digest == req.payload_digest


def test_suspicious_primary_send_same_request_with_same_signatures(
        looper, txnPoolNodeSet, sdk_pool_handle, two_requests, sdk_wallet_stewards):
    # sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_stewards[0], sdk_pool_handle)
    old_view = txnPoolNodeSet[0].viewNo

    replica = getPrimaryReplica(txnPoolNodeSet)
    ppReq = replica.create_3pc_batch(DOMAIN_LEDGER_ID)
    digest = 'some_digest'

    def get_payload_digest(txn):
        return txn

    uncommittedTxns = []

    def reduced_validation(self, request):
        ledger_id, seq_no = self.seqNoDB.get_by_payload_digest(request.payload_digest)
        if ledger_id is not None and seq_no is not None:
            raise SuspiciousPrePrepare('Trying to order already ordered request')

        for txn in uncommittedTxns:
            if get_payload_digest(txn) == request.payload_digest:
                raise SuspiciousPrePrepare('Trying to order already ordered request')

    def modified_apply(self, request, cons_time):
        uncommittedTxns.append(request.payload_digest)

    counter = 0

    def incr(self, ledgerId, stateRootHash, reqCount):
        nonlocal counter
        counter += 1

    for node in txnPoolNodeSet:
        node.doDynamicValidation = types.MethodType(reduced_validation, node)
        node.applyReq = types.MethodType(modified_apply, node)
        node.master_replica.revert = types.MethodType(incr, node.master_replica)

        # So nodes have this digest in requests list
        state = node.requests.add(FakeSomething(key=digest))
        state.finalised = FakeSomething(key=digest,
                                        identifier='',
                                        reqId='',
                                        digest=digest,
                                        payload_digest=digest + 'a',
                                        operation={TXN_TYPE: 1})

    ppReq.reqIdr = [digest, digest]
    ppReq._fields['reqIdr'] = [digest, digest]
    replica.sendPrePrepare(ppReq)

    def reverts():
        assert counter == 3

    looper.run(eventually(reverts))
