from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

nodeCount = 4
nodes_wth_bls = 4

def test_each_node_has_bls(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        assert node.replicas[0]._bls_bft


def test_send_txns_full_bls(tconf, looper, txnPoolNodeSet,
                            client1, client1Connected, wallet1):
    number_of_requests = 2  # at least two because first request could have no
    # signature since state can be clear

    # Using loop to avoid 3pc batching
    for i in range(number_of_requests):
        reqs = sendRandomRequests(wallet1, client1, 1)
        waitForSufficientRepliesForRequests(looper, client1, requests=reqs)

    # for node in txnPoolNodeSet:
    #     state_root_hash = node.getState(DOMAIN_LEDGER_ID).committedHeadHash
    #     assert node.bls_store.get(state_root_hash)

