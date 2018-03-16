from plenum.test.bls.helper import check_update_bls_key

nodeCount = 4
nodes_wth_bls = 4


# As we use tests with Module scope, results from previous tests are accumulated, so
# rotating BLS keys one by one, eventually we will have all keys changed

def test_update_bls_one_node(looper, txnPoolNodeSet,
                             sdk_wallet_stewards,
                             sdk_wallet_client,
                             sdk_pool_handle):
    '''
    Rotated BLS key for 1st node;
    BLS multi-signatures must be calculated for all Nodes.
    '''
    check_update_bls_key(node_num=0, saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle)


def test_update_bls_two_nodes(looper, txnPoolNodeSet,
                              sdk_wallet_stewards,
                              sdk_wallet_client,
                              sdk_pool_handle):
    '''
    Rotated BLS key for 1st and 2d nodes;
    BLS multi-signatures must be calculated for all Nodes.
    '''
    check_update_bls_key(node_num=1, saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle)


def test_update_bls_three_nodes(looper, txnPoolNodeSet,
                                sdk_wallet_stewards,
                                sdk_wallet_client,
                                sdk_pool_handle):
    '''
    Rotated BLS key for 1-3 Nodes;
    BLS multi-signatures must be calculated for all Nodes.
    '''
    check_update_bls_key(node_num=2, saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle)


def test_update_bls_all_nodes(looper, txnPoolNodeSet,
                              sdk_wallet_stewards,
                              sdk_wallet_client,
                              sdk_pool_handle):
    '''
    Rotated BLS key for all Nodes;
    BLS multi-signatures must be calculated for all Nodes.
    '''
    check_update_bls_key(node_num=3, saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle)
