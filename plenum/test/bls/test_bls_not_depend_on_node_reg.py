from plenum.test.pool_transactions.helper import demote_node
from plenum.test.test_node import TestNode, checkNodesConnected, ensureElectionsDone, ensure_node_disconnected
from plenum.test.helper import sdk_send_batches_of_random_and_check

from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.types import f

nodeCount = 7


def test_bls_not_depend_on_node_reg(looper, txnPoolNodeSet,
                                    sdk_pool_handle, sdk_wallet_client):
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet,
                                         sdk_pool_handle, sdk_wallet_client, 1, 2)

    node = txnPoolNodeSet[2]
    last_pre_prepare = \
        node.master_replica.prePrepares[node.master_replica.last_ordered_3pc]

    bls = getattr(last_pre_prepare, f.BLS_MULTI_SIG.nm)

    # Get random participant
    node_name = next(iter(bls[1]))

    # We've removed one of the nodes from another node's log
    del node.nodeReg[node_name]

    # Still we can validate Preprepare
    assert node.master_replica._bls_bft_replica._bls_bft.bls_key_register.get_key_by_name(node_name)


def test_bls_not_depend_on_node_reg_demotion(looper, txnPoolNodeSet,
                                             sdk_pool_handle, sdk_wallet_client, tdir, tconf, allPluginsPath,
                                             sdk_wallet_stewards):
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet,
                                         sdk_pool_handle, sdk_wallet_client, 1, 2)

    primary_node = txnPoolNodeSet[0]
    node_to_stop = txnPoolNodeSet[3]
    node_to_demote = txnPoolNodeSet[4]
    txnPoolNodeSet.remove(node_to_demote)

    last_pre_prepare = \
        node_to_stop.master_replica.prePrepares[node_to_stop.master_replica.last_ordered_3pc]
    bls = getattr(last_pre_prepare, f.BLS_MULTI_SIG.nm)
    state_root_hash = bls[0]

    node_to_stop.cleanupOnStopping = True
    node_to_stop.stop()
    looper.removeProdable(node_to_stop)
    ensure_node_disconnected(looper, node_to_stop, txnPoolNodeSet, timeout=2)

    demote_node(looper, sdk_wallet_stewards[4], sdk_pool_handle, node_to_demote)

    config_helper = PNodeConfigHelper(node_to_stop.name, tconf, chroot=tdir)
    restarted_node = TestNode(node_to_stop.name, config_helper=config_helper, config=tconf,
                              pluginPaths=allPluginsPath, ha=node_to_stop.nodestack.ha,
                              cliha=node_to_stop.clientstack.ha)
    looper.add(restarted_node)
    txnPoolNodeSet[3] = restarted_node
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet,
                                         sdk_pool_handle, sdk_wallet_client, 1, 1)

    def get_current_bls_keys(node):
        return node.master_replica._bls_bft_replica._bls_bft.bls_key_register._current_bls_keys

    restarted_node.master_replica._bls_bft_replica._bls_bft.bls_key_register._load_keys_for_root(state_root_hash)

    assert get_current_bls_keys(restarted_node) == get_current_bls_keys(primary_node)
