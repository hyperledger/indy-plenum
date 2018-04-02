from plenum.test.bls.helper import sdk_check_bls_multi_sig_after_send

nodeCount = 4
nodes_wth_bls = 0


def test_each_node_has_bls(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        assert node.bls_bft
        assert node.replicas[0]._bls_bft_replica


def test_send_txns_no_bls(looper, txnPoolNodeSet,
                          sdk_pool_handle, sdk_wallet_client):
    sdk_check_bls_multi_sig_after_send(looper, txnPoolNodeSet,
                                       sdk_pool_handle, sdk_wallet_client,
                                       saved_multi_sigs_count=0)
