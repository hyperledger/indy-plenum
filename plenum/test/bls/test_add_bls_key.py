import functools

from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.quorums import Quorum
from plenum.test.bls.helper import check_update_bls_key

nodeCount = 4
nodes_wth_bls = 0


# As we use tests with Module scope, results from previous tests are accumulated, so
# rotating BLS keys one by one, eventually we will have all keys changed

def test_add_bls_one_node(looper,
                          txnPoolNodeSet,
                          sdk_pool_handle,
                          sdk_wallet_stewards, sdk_wallet_client):
    '''
    Added BLS key for 1st Node;
    do not expect that BLS multi-sigs are applied since no consensus (n-f)
    '''
    check_update_bls_key(node_num=0,
                         saved_multi_sigs_count=0,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle)


def test_add_bls_two_nodes(looper,
                           txnPoolNodeSet,
                           sdk_pool_handle,
                           sdk_wallet_stewards,
                           sdk_wallet_client):
    '''
    Added BLS key for 1st and 2d Nodes;
    do not expect that BLS multi-sigs are applied since no consensus (n-f)
    '''
    check_update_bls_key(node_num=1,
                         saved_multi_sigs_count=0,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle)


def test_add_bls_three_nodes(looper,
                             txnPoolNodeSet,
                             sdk_pool_handle,
                             sdk_wallet_stewards,
                             sdk_wallet_client):
    '''
    Added BLS key for 1st, 2d and 3d Nodes;
    expect that BLS multi-sigs are applied since we have consensus now (3=n-f)
    '''
    # make sure that we have commits from all nodes, and have 3 of 4 (n-f) BLS sigs there is enough
    # otherwise we may have 3 commits, but 1 of them may be without BLS, so we will Order this txn, but without multi-sig

    def patched_set_validators(self, validators):
        ConsensusSharedData.set_validators(self, validators)
        self.quorums.commit = Quorum(nodeCount)

    for node in txnPoolNodeSet:
        for r in node.replicas.values():
            r._consensus_data.quorums.commit = Quorum(nodeCount)
            r._consensus_data.set_validators = functools.partial(patched_set_validators, r._consensus_data)
        node.quorums.commit = Quorum(nodeCount)
    check_update_bls_key(node_num=2,
                         saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle)


def test_add_bls_all_nodes(looper,
                           txnPoolNodeSet,
                           sdk_pool_handle,
                           sdk_wallet_stewards,
                           sdk_wallet_client):
    '''
    Eventually added BLS key for all Nodes;
    expect that BLS multi-sigs are applied since we have consensus now (4 > n-f)
    '''
    check_update_bls_key(node_num=3,
                         saved_multi_sigs_count=4,
                         looper=looper, txnPoolNodeSet=txnPoolNodeSet,
                         sdk_wallet_stewards=sdk_wallet_stewards,
                         sdk_wallet_client=sdk_wallet_client,
                         sdk_pool_handle=sdk_pool_handle)
