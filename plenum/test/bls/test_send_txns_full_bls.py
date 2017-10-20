from plenum.test.bls.helper import check_bls_multi_sig_after_send
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

nodeCount = 7
nodes_wth_bls = 7

def test_each_node_has_bls(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        assert node.bls_bft
        assert node.replicas[0]._bls_bft_replica


def test_send_txns_full_bls(looper, txnPoolNodeSet,
                            client1, client1Connected, wallet1):
    check_bls_multi_sig_after_send(looper, txnPoolNodeSet,
                                   client1, wallet1,
                                   saved_multi_sigs_count=nodeCount)