from plenum.common.constants import ROOT_HASH, MULTI_SIGNATURE, PROOF_NODES, TXN_TYPE, DATA, TXN_TIME, STATE_PROOF, \
    DOMAIN_LEDGER_ID, MULTI_SIGNATURE_VALUE, MULTI_SIGNATURE_PARTICIPANTS, MULTI_SIGNATURE_SIGNATURE, MULTI_SIGNATURE_VALUE_LEDGER_ID, \
    MULTI_SIGNATURE_VALUE_STATE_ROOT, MULTI_SIGNATURE_VALUE_TXN_ROOT, MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT, \
    MULTI_SIGNATURE_VALUE_TIMESTAMP, DOMAIN_LEDGER_ID
from plenum.common.plenum_protocol_version import PlenumProtocolVersion
from plenum.common.request import SafeRequest
from plenum.common.types import f
from plenum.common.util import get_utc_epoch
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests, wait_for_requests_ordered, \
    randomOperation
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

nodeCount = 4
nodes_wth_bls = 4


def check_result(txnPoolNodeSet, req, client, should_have_proof):
    for node in txnPoolNodeSet:
        req_handler = node.get_req_handler(DOMAIN_LEDGER_ID)
        key = req_handler.prepare_buy_key(req.identifier, req.reqId)
        proof = req_handler.make_proof(key)

        txn_time = get_utc_epoch()
        result = req_handler.make_result(req,
                                         {TXN_TYPE: "buy"},
                                         2,
                                         txn_time,
                                         proof)
        assert result
        assert result[DATA] == {TXN_TYPE: "buy"}
        assert result[f.IDENTIFIER.nm] == req.identifier
        assert result[f.REQ_ID.nm] == req.reqId
        assert result[f.SEQ_NO.nm] == 2
        assert result[TXN_TIME] == txn_time

        if should_have_proof:
            assert result[STATE_PROOF] == proof
            assert client.validate_proof(result)
        else:
            assert STATE_PROOF not in result


def test_make_proof_bls_enabled(looper, txnPoolNodeSet,
                                client1, client1Connected, wallet1):
    reqs = sendRandomRequests(wallet1, client1, 1)
    wait_for_requests_ordered(looper, txnPoolNodeSet, reqs)

    req = reqs[0]
    for node in txnPoolNodeSet:
        req_handler = node.get_req_handler(DOMAIN_LEDGER_ID)
        key = req_handler.prepare_buy_key(req.identifier, req.reqId)
        proof = req_handler.make_proof(key)
        assert proof
        assert ROOT_HASH in proof
        assert MULTI_SIGNATURE in proof
        assert PROOF_NODES in proof

        multi_sig = proof[MULTI_SIGNATURE]
        assert MULTI_SIGNATURE_SIGNATURE in multi_sig
        assert MULTI_SIGNATURE_PARTICIPANTS in multi_sig
        assert MULTI_SIGNATURE_VALUE in multi_sig

        multi_sig_value = multi_sig[MULTI_SIGNATURE_VALUE]
        assert MULTI_SIGNATURE_VALUE_LEDGER_ID in multi_sig_value
        assert MULTI_SIGNATURE_VALUE_STATE_ROOT in multi_sig_value
        assert MULTI_SIGNATURE_VALUE_TXN_ROOT in multi_sig_value
        assert MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT in multi_sig_value
        assert MULTI_SIGNATURE_VALUE_TIMESTAMP in multi_sig_value
        # check that multi sig values are in order
        value_keys = list(multi_sig_value.keys())
        assert [MULTI_SIGNATURE_VALUE_LEDGER_ID,
                MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT,
                MULTI_SIGNATURE_VALUE_STATE_ROOT,
                MULTI_SIGNATURE_VALUE_TIMESTAMP,
                MULTI_SIGNATURE_VALUE_TXN_ROOT] == value_keys

        assert client1.validate_multi_signature(proof)


def test_make_result_bls_enabled(looper, txnPoolNodeSet,
                                 client1, client1Connected, wallet1):
    reqs = sendRandomRequests(wallet1, client1, 1)
    wait_for_requests_ordered(looper, txnPoolNodeSet, reqs)
    req = reqs[0]

    assert req.protocolVersion
    assert req.protocolVersion >= PlenumProtocolVersion.STATE_PROOF_SUPPORT.value
    check_result(txnPoolNodeSet, req, client1, True)


def test_make_result_no_protocol_version(looper, txnPoolNodeSet,
                                         client1, client1Connected, wallet1):
    request = SafeRequest(identifier="1" * 16,
                          reqId=1,
                          operation=randomOperation(),
                          signature="signature")
    request.protocolVersion = False
    check_result(txnPoolNodeSet, request, client1, False)


def test_make_result_protocol_version_less_than_state_proof(looper,
                                                            txnPoolNodeSet,
                                                            client1,
                                                            client1Connected,
                                                            wallet1):
    request = SafeRequest(identifier="1" * 16,
                          reqId=1,
                          operation=randomOperation(),
                          signature="signature")
    request.protocolVersion = 0
    check_result(txnPoolNodeSet, request, client1, False)


def test_make_result_no_protocol_version_in_request_by_default(looper,
                                                               txnPoolNodeSet,
                                                               client1,
                                                               client1Connected,
                                                               wallet1):
    request = SafeRequest(identifier="1" * 16,
                          reqId=1,
                          operation=randomOperation(),
                          signature="signature")
    check_result(txnPoolNodeSet, request, client1, False)


def test_proof_in_reply(looper, txnPoolNodeSet,
                        client1, client1Connected, wallet1):
    reqs = sendRandomRequests(wallet1, client1, 1)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)

    req = reqs[0]
    result = client1.getReply(req.identifier, req.reqId)[0]

    assert result
    assert result[TXN_TYPE] == "buy"
    assert result[f.IDENTIFIER.nm] == req.identifier
    assert result[f.REQ_ID.nm] == req.reqId
    assert result[f.SEQ_NO.nm]
    assert result[TXN_TIME]
    assert STATE_PROOF in result

    state_proof = result[STATE_PROOF]
    assert ROOT_HASH in state_proof
    assert MULTI_SIGNATURE in state_proof
    assert PROOF_NODES in state_proof

    multi_sig = state_proof[MULTI_SIGNATURE]
    assert MULTI_SIGNATURE_SIGNATURE in multi_sig
    assert MULTI_SIGNATURE_PARTICIPANTS in multi_sig
    assert MULTI_SIGNATURE_VALUE in multi_sig

    multi_sig_value = multi_sig[MULTI_SIGNATURE_VALUE]
    assert MULTI_SIGNATURE_VALUE_LEDGER_ID in multi_sig_value
    assert MULTI_SIGNATURE_VALUE_STATE_ROOT in multi_sig_value
    assert MULTI_SIGNATURE_VALUE_TXN_ROOT in multi_sig_value
    assert MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT in multi_sig_value
    assert MULTI_SIGNATURE_VALUE_TIMESTAMP in multi_sig_value

    assert client1.validate_multi_signature(state_proof)
    assert client1.validate_proof(result)


def test_make_proof_committed_head_used(looper, txnPoolNodeSet,
                                        client1, client1Connected, wallet1):
    reqs = sendRandomRequests(wallet1, client1, 1)
    wait_for_requests_ordered(looper, txnPoolNodeSet, reqs)
    req = reqs[0]
    req_handler = txnPoolNodeSet[0].get_req_handler(DOMAIN_LEDGER_ID)
    key = req_handler.prepare_buy_key(req.identifier, req.reqId)

    for node in txnPoolNodeSet:
        node.states[DOMAIN_LEDGER_ID].set(key, b'somevalue')

    check_result(txnPoolNodeSet, req, client1, True)
