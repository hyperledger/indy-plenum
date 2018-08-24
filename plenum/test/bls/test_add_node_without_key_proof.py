from plenum.common.keygen_utils import init_bls_keys
from plenum.common.util import hexToFriendly
from plenum.server.quorums import Quorum
from plenum.test.bls.helper import check_update_bls_key
from plenum.test.helper import sdk_send_random_request, sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import sdk_send_update_node, sdk_pool_refresh

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
                           sdk_wallet_client,
                           monkeypatch):
    '''
    Added BLS key for 1st and 2d Nodes;
    do not expect that BLS multi-sigs are applied since no consensus (n-f)
    '''
    for n in txnPoolNodeSet:
        monkeypatch.setattr(n.poolManager.reqHandler, 'doStaticValidation', lambda req: True)
    for node_index in range(0, 3):
        update_bls_keys(node_index, sdk_wallet_stewards, sdk_pool_handle, looper, txnPoolNodeSet)
    monkeypatch.undo()
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_stewards[0], 5)


def update_bls_keys(node_index, sdk_wallet_stewards, sdk_pool_handle, looper, txnPoolNodeSet):
    node = txnPoolNodeSet[node_index]
    sdk_wallet_steward = sdk_wallet_stewards[node_index]
    new_blspk, key_proof = init_bls_keys(node.keys_dir, node.name)
    node_dest = hexToFriendly(node.nodestack.verhex)
    sdk_send_update_node(looper, sdk_wallet_steward,
                         sdk_pool_handle,
                         node_dest, node.name,
                         None, None,
                         None, None,
                         bls_key=new_blspk,
                         services=None,
                         key_proof=None)
    poolSetExceptOne = list(txnPoolNodeSet)
    poolSetExceptOne.remove(node)
    waitNodeDataEquality(looper, node, *poolSetExceptOne)
    sdk_pool_refresh(looper, sdk_pool_handle)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)