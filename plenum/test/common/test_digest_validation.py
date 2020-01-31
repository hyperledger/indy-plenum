import json
import types

import pytest
from indy.did import create_and_store_my_did

from plenum.server.consensus.ordering_service import OrderingService

from plenum.common.messages.node_messages import InstanceChange

from plenum.server.suspicion_codes import Suspicions
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import getPrimaryReplica
from plenum.common.exceptions import RequestNackedException
from plenum.common.request import Request
from plenum.test.helper import sdk_gen_request, sdk_multisign_request_object, sdk_send_signed_requests, \
    sdk_get_and_check_replies, sdk_random_request_objects, waitForViewChange, max_3pc_batch_limits, \
    sdk_send_random_and_check

from plenum.common.constants import CURRENT_PROTOCOL_VERSION, DOMAIN_LEDGER_ID, TXN_TYPE

from plenum.common.util import randomString
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


def malicious_dynamic_validation(node, request: Request, req_pp_time: int):

    node.write_manager.do_taa_validation(request, req_pp_time=req_pp_time, config=node.config)

    operation = request.operation
    req_handler = node.write_manager.request_handlers[operation[TXN_TYPE]][0]
    req_handler.dynamic_validation(request, req_pp_time)


def wait_one_batch(node, before):
    assert node.replicas[0].last_ordered_3pc[1] == before[0] + 1
    assert node.replicas[1].last_ordered_3pc[1] == before[1] + 1


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
    e.match('Same txn was already ordered with different signatures or pluggable fields')


def test_send_same_txn_with_different_signatures_in_one_batch(
        looper, txnPoolNodeSet, sdk_pool_handle, two_requests, tconf):
    req1, req2 = two_requests

    lo_before = (txnPoolNodeSet[0].replicas[0].last_ordered_3pc[1],
                 txnPoolNodeSet[0].replicas[1].last_ordered_3pc[1])

    old_reqs = len(txnPoolNodeSet[0].requests)
    with max_3pc_batch_limits(tconf, size=2):
        sdk_send_signed_requests(sdk_pool_handle, [req1])
        sdk_send_signed_requests(sdk_pool_handle, [req2])

        # We need to check for ordering this way, cause sdk do not allow
        # track two requests with same reqId at the same time
        looper.run(eventually(wait_one_batch, txnPoolNodeSet[0], lo_before))

    assert len(txnPoolNodeSet[0].requests) == old_reqs + 2

    pps = txnPoolNodeSet[0].master_replica._ordering_service.sent_preprepares
    pp = pps[pps.keys()[-1]]
    idrs = pp.reqIdr
    assert len(idrs) == 1
    assert Request(**json.loads(req1)).digest in idrs


def test_parts_of_nodes_have_same_request_with_different_signatures(
        looper, txnPoolNodeSet, sdk_pool_handle, two_requests, sdk_wallet_stewards, tconf):
    req1s, req2s = two_requests
    req1 = Request(**json.loads(req1s))
    req2 = Request(**json.loads(req2s))

    lo_before = (txnPoolNodeSet[0].replicas[0].last_ordered_3pc[1],
                 txnPoolNodeSet[0].replicas[1].last_ordered_3pc[1])

    for node in txnPoolNodeSet[0:2]:
        req_state = node.requests.add(req1)
        req_state.propagates['Alpha'] = req1
        req_state.propagates['Beta'] = req1
        req_state.propagates['Gamma'] = req1
        req_state.propagates['Delta'] = req1
        node.tryForwarding(req1)
        assert node.requests[req1.key].forwarded

    for node in txnPoolNodeSet[2:4]:
        req_state = node.requests.add(req2)
        req_state.propagates['Alpha'] = req2
        req_state.propagates['Beta'] = req2
        req_state.propagates['Gamma'] = req2
        req_state.propagates['Delta'] = req2
        node.tryForwarding(req2)
        assert node.requests[req2.key].forwarded

    looper.run(eventually(wait_one_batch, txnPoolNodeSet[0], lo_before))
    for node in txnPoolNodeSet[0:2]:
        assert node.spylog.count(node.request_propagates) == 0
    for node in txnPoolNodeSet[2:4]:
        assert node.spylog.count(node.request_propagates) >= 1
        node.spylog.getAll(node.request_propagates)

    req1s = sdk_send_signed_requests(sdk_pool_handle, [req1s])
    sdk_get_and_check_replies(looper, req1s)

    req2s = sdk_send_signed_requests(sdk_pool_handle, [req2s])
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, req2s)
    e.match('Same txn was already ordered with different signatures or pluggable fields')

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)


def test_suspicious_primary_send_same_request_with_different_signatures(
        looper, txnPoolNodeSet, sdk_pool_handle, two_requests):
    assert txnPoolNodeSet[0].master_replica.isPrimary
    txnPoolNodeSet[0].master_replica._ordering_service._do_dynamic_validation = \
        types.MethodType(malicious_dynamic_validation,
                         txnPoolNodeSet[0])

    req1, req2 = two_requests

    old_view = txnPoolNodeSet[0].viewNo
    sdk_send_signed_requests(sdk_pool_handle, [req1])
    sdk_send_signed_requests(sdk_pool_handle, [req2])

    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=old_view + 1)
    all(cll.params['msg'][1] == Suspicions.PPR_WITH_ORDERED_REQUEST.code for cll in
        txnPoolNodeSet[0].spylog.getAll('sendToViewChanger') if isinstance(cll.params['msg'], InstanceChange))
    txnPoolNodeSet[0].master_replica._ordering_service._do_dynamic_validation = \
        types.MethodType(OrderingService._do_dynamic_validation,
                         txnPoolNodeSet[0].master_replica._ordering_service)


def test_suspicious_primary_send_same_request_with_same_signatures(
        looper, txnPoolNodeSet, sdk_pool_handle, two_requests, sdk_wallet_stewards, tconf):
    couple = sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards[0], 1)[0]
    req = Request(**couple[0])
    replica = getPrimaryReplica(txnPoolNodeSet)
    replica._ordering_service._do_dynamic_validation = types.MethodType(malicious_dynamic_validation, replica.node)

    txnPoolNodeSet.remove(replica.node)
    old_suspicious = {}
    for i, node in enumerate(txnPoolNodeSet):
        old_suspicious[i] = node.master_replica._ordering_service.spylog.count(OrderingService.report_suspicious_node)
        node.seqNoDB._keyValueStorage.remove(req.digest)
        node.seqNoDB._keyValueStorage.remove(req.payload_digest)

    ppReq = replica._ordering_service.create_3pc_batch(DOMAIN_LEDGER_ID)
    ppReq._fields['reqIdr'] = [req.digest, req.digest]
    replica._ordering_service.send_pre_prepare(ppReq)

    def reverts():
        for i, node in enumerate(txnPoolNodeSet):
            assert old_suspicious[i] + 1 == node.master_replica._ordering_service.\
                spylog.count(OrderingService.report_suspicious_node)

    looper.run(eventually(reverts))
