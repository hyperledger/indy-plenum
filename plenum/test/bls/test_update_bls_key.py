from plenum.test.bls.helper import check_update_bls_key
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

nodeCount = 4
nodes_wth_bls = 4


# As we use tests with Module scope, results from previous tests are accumulated, so
# rotating BLS keys one by one, eventually we will have all keys changed

def test_update_bls_one_node(looper, txnPoolNodeSet, client_tdir,
                             poolTxnClientData, stewards_and_wallets):
    '''
    Rotated BLS key for 1st node;
    BLS multi-signatures must be calculated for all Nodes.
    '''
    check_update_bls_key(node_num=0,
                         saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         client_tdir=client_tdir,
                         poolTxnClientData=poolTxnClientData,
                         stewards_and_wallets=stewards_and_wallets)


def test_update_bls_two_nodes(looper, txnPoolNodeSet, client_tdir,
                              poolTxnClientData, stewards_and_wallets):
    '''
    Rotated BLS key for 1st and 2d nodes;
    BLS multi-signatures must be calculated for all Nodes.
    '''
    check_update_bls_key(node_num=1,
                         saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         client_tdir=client_tdir,
                         poolTxnClientData=poolTxnClientData,
                         stewards_and_wallets=stewards_and_wallets)


def test_update_bls_three_nodes(looper, txnPoolNodeSet, client_tdir,
                                poolTxnClientData, stewards_and_wallets):
    '''
    Rotated BLS key for 1-3 Nodes;
    BLS multi-signatures must be calculated for all Nodes.
    '''
    check_update_bls_key(node_num=2,
                         saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         client_tdir=client_tdir,
                         poolTxnClientData=poolTxnClientData,
                         stewards_and_wallets=stewards_and_wallets)


def test_update_bls_all_nodes(looper, txnPoolNodeSet, client_tdir,
                              poolTxnClientData, stewards_and_wallets):
    '''
    Rotated BLS key for all Nodes;
    BLS multi-signatures must be calculated for all Nodes.
    '''
    check_update_bls_key(node_num=3,
                         saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         client_tdir=client_tdir,
                         poolTxnClientData=poolTxnClientData,
                         stewards_and_wallets=stewards_and_wallets)
