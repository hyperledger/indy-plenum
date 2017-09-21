from common.serializers.serialization import state_roots_serializer
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

nodeCount = 4
nodes_wth_bls = 2


def test_send_txns_partial_bls(tconf, looper, txnPoolNodeSet,
                          client1, client1Connected, wallet1):
    number_of_requests = 2  # at least two because first request could have no
    # signature since state can be clear

    # Using loop to avoid 3pc batching
    state_roots = []
    for i in range(number_of_requests):
        reqs = sendRandomRequests(wallet1, client1, 1)
        waitForSufficientRepliesForRequests(looper, client1, requests=reqs)
        state_roots.append(
            state_roots_serializer.serialize(
                bytes(txnPoolNodeSet[0].getState(DOMAIN_LEDGER_ID).committedHeadHash)))

    # nothing should be saved since we have less nodes with BLS than consensus (n-f)
    for node in txnPoolNodeSet:
        for state_root in state_roots:
            assert not node.bls_store.get(state_root)
