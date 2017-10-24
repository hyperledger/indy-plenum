from plenum.common.constants import ROOT_HASH, MULTI_SIGNATURE, PROOF_NODES, TXN_TYPE, DATA, TXN_TIME, STATE_PROOF
from plenum.common.types import f
from plenum.common.util import get_utc_epoch
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

nodeCount = 4
nodes_wth_bls = 4


def test_make_proof_bls_enabled(looper, txnPoolNodeSet,
                                client1, client1Connected, wallet1):
    reqs = sendRandomRequests(wallet1, client1, 1)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)

    req = reqs[0]
    for node in txnPoolNodeSet:
        key = node.reqHandler.prepare_buy_key(req.identifier, req.reqId)
        proof = node.reqHandler.make_proof(key)
        assert proof
        assert ROOT_HASH in proof
        assert MULTI_SIGNATURE in proof
        assert PROOF_NODES in proof
        assert client1.validate_multi_signature(proof)


def test_make_result_bls_enabled(looper, txnPoolNodeSet,
                                 client1, client1Connected, wallet1):
    reqs = sendRandomRequests(wallet1, client1, 1)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)

    req = reqs[0]
    for node in txnPoolNodeSet:
        key = node.reqHandler.prepare_buy_key(req.identifier, req.reqId)
        proof = node.reqHandler.make_proof(key)

        txn_time = get_utc_epoch()
        result = node.reqHandler.make_result(req,
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
        assert result[STATE_PROOF] == proof

        assert client1.validate_proof(result)


def test_make_result_no_protocol_version(looper, txnPoolNodeSet,
                                         client1, client1Connected, wallet1):
    reqs = sendRandomRequests(wallet1, client1, 1)
    for req in reqs:
        req.protocolVersion = None
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)

    req = reqs[0]
    for node in txnPoolNodeSet:
        key = node.reqHandler.prepare_buy_key(req.identifier, req.reqId)
        proof = node.reqHandler.make_proof(key)

        txn_time = get_utc_epoch()
        result = node.reqHandler.make_result(req,
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
        assert STATE_PROOF not in proof
