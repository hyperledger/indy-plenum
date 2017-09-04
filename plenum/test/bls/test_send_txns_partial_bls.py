from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

nodeCount = 4
nodes_wth_bls = 2


def test_send_txns_partial_bls(tconf, looper, txnPoolNodeSet,
                          client1, client1Connected, wallet1):
    reqs = sendRandomRequests(wallet1, client1, 10)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)
