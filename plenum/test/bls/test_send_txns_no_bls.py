from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

nodeCount = 4
nodes_wth_bls = 0


def test_send_txns_no_bls(tconf, looper, txnPoolNodeSet,
                          client1, client1Connected, wallet1):
    number_of_requests = 2  # at least two because first request could have no
    # signature since state can be clear

    # Using loop to avoid 3pc batching
    for i in range(number_of_requests):
        reqs = sendRandomRequests(wallet1, client1, 1)
        waitForSufficientRepliesForRequests(looper, client1, requests=reqs)
