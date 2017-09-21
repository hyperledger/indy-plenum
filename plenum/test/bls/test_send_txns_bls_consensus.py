from common.serializers.serialization import state_roots_serializer
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.server.quorums import Quorum
from plenum.test.bls.helper import check_bls_multi_sig_after_send
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

nodeCount = 7
nodes_wth_bls = 5


def test_send_txns_bls_consensus(tconf, looper, txnPoolNodeSet,
                          client1, client1Connected, wallet1):
    # make sure that we have commits from all nodes, and have 5 of 7 (n-f) BLS sigs there is enough
    for node in txnPoolNodeSet:
        node.quorums.commit = Quorum(nodeCount)
    check_bls_multi_sig_after_send(looper, txnPoolNodeSet,
                                   client1, wallet1,
                                   saved_multi_sigs_count=nodes_wth_bls)