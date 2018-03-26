from plenum.server.quorums import Quorum
from plenum.test.bls.helper import sdk_check_bls_multi_sig_after_send

nodeCount = 7
nodes_wth_bls = 4


def test_each_node_has_bls(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        assert node.bls_bft
        assert node.replicas[0]._bls_bft_replica


def test_send_txns_bls_less_than_consensus(looper, txnPoolNodeSet,
                                           sdk_pool_handle, sdk_wallet_client):
    # make sure that we have commits from all nodes, and have 4 of 7 ( < n-f) BLS sigs there is not enough
    for node in txnPoolNodeSet:
        node.quorums.commit = Quorum(nodeCount)
    sdk_check_bls_multi_sig_after_send(looper, txnPoolNodeSet,
                                       sdk_pool_handle, sdk_wallet_client,
                                       saved_multi_sigs_count=0)
