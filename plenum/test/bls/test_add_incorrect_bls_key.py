from plenum.server.quorums import Quorum
from plenum.test.bls.helper import check_update_bls_key

nodeCount = 4
nodes_wth_bls = 0


# As we use tests with Module scope, results from previous tests are accumulated, so
# rotating BLS keys one by one, eventually we will have all keys changed

def test_add_incorrect_bls_one_node(looper,
                                    txnPoolNodeSet,
                                    sdk_pool_handle,
                                    sdk_wallet_stewards,
                                    sdk_wallet_client):
    '''
    Added wrong BLS key for 1st Node;
    do not expect that BLS multi-sigs are applied
    '''
    check_update_bls_key(node_num=0,
                         saved_multi_sigs_count=0,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle,
                         add_wrong=True)


def test_add_incorrect_bls_two_nodes(looper,
                                     txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_stewards,
                                     sdk_wallet_client):
    '''
    Added wrong BLS key for 1st and 2d Nodes;
    do not expect that BLS multi-sigs are applied
    '''
    check_update_bls_key(node_num=1,
                         saved_multi_sigs_count=0,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle,
                         add_wrong=True)


def test_add_incorrect_bls_three_nodes(looper,
                                       txnPoolNodeSet,
                                       sdk_pool_handle,
                                       sdk_wallet_stewards,
                                       sdk_wallet_client):
    '''
    Added wrong BLS key for 1-3 Nodes;
    do not expect that BLS multi-sigs are applied
    '''
    # make sure that we have commits from all nodes, and have 3 of 4 (n-f) BLS sigs there is enough
    # otherwise we may have 3 commits, but 1 of them may be without BLS, so we will Order this txn, but without multi-sig
    for node in txnPoolNodeSet:
        node.quorums.commit = Quorum(nodeCount)
    check_update_bls_key(node_num=2,
                         saved_multi_sigs_count=0,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle,
                         add_wrong=True)


def test_add_incorrect_bls_all_nodes(looper,
                                     txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_stewards,
                                     sdk_wallet_client):
    '''
    Added wrong BLS key for all Nodes;
    Still do not expect that BLS multi-sigs are applied
    '''
    check_update_bls_key(node_num=3,
                         saved_multi_sigs_count=0,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle,
                         add_wrong=True)
