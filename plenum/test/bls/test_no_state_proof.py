from plenum.common.constants import TXN_TYPE, STATE_PROOF
from plenum.common.util import get_utc_epoch
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

nodeCount = 4
nodes_wth_bls = 0


def test_make_proof_bls_disabled(looper, txnPoolNodeSet,
                                 client1, client1Connected, wallet1):
    reqs = sendRandomRequests(wallet1, client1, 1)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)

    req = reqs[0]
    for node in txnPoolNodeSet:
        key = node.reqHandler.prepare_buy_key(req.identifier)
        proof = node.reqHandler.make_proof(key)
        assert not proof


def test_make_result_bls_disabled(looper, txnPoolNodeSet,
                                  client1, client1Connected, wallet1):
    reqs = sendRandomRequests(wallet1, client1, 1)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)

    req = reqs[0]
    for node in txnPoolNodeSet:
        key = node.reqHandler.prepare_buy_key(req.identifier)
        proof = node.reqHandler.make_proof(key)
        result = node.reqHandler.make_result(req,
                                             {TXN_TYPE: "buy"},
                                             2,
                                             get_utc_epoch(),
                                             proof)
        assert STATE_PROOF not in result
