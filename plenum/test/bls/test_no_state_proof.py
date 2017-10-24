from plenum.common.constants import ROOT_HASH, MULTI_SIGNATURE, PROOF_NODES
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

nodeCount = 4
nodes_wth_bls = 0


def test_no_state_proof_if_no_multi_sig(looper, txnPoolNodeSet,
                                        client1, client1Connected, wallet1):
    reqs = sendRandomRequests(wallet1, client1, 1)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)

    req = reqs[0]
    for node in txnPoolNodeSet:
        key = node.reqHandler.prepare_buy_key(req.identifier, req.reqId)
        proof = node.reqHandler.make_proof(key)
        assert not proof
