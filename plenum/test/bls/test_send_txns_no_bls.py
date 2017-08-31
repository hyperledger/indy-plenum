from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

def test_send_txns_no_bls(tconf, looper, txnPoolNodeSet,
                          client1, client1Connected, wallet1):
    reqs = sendRandomRequests(wallet1, client1, tconf.Max3PCBatchSize)
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)
