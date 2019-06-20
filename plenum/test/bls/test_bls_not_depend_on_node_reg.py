from copy import deepcopy

from crypto.bls.bls_multi_signature import MultiSignature

from common.serializers.base58_serializer import Base58Serializer

from plenum.test.pool_transactions.helper import demote_node
from plenum.test.test_node import TestNode, checkNodesConnected, ensureElectionsDone, ensure_node_disconnected
from plenum.test.helper import sdk_send_batches_of_random_and_check

from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.types import f

nodeCount = 4
serializer = Base58Serializer()


def test_bls_not_depend_on_node_reg(looper, txnPoolNodeSet,
                                    sdk_pool_handle, sdk_wallet_client):
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet,
                                         sdk_pool_handle, sdk_wallet_client, 3, 3)

    node = txnPoolNodeSet[2]
    last_pre_prepare = \
        node.master_replica.prePrepares[node.master_replica.last_ordered_3pc]

    bls = getattr(last_pre_prepare, f.BLS_MULTI_SIG.nm)

    # Get random participant
    node_name = next(iter(bls[1]))

    # We've removed one of the nodes from another node's log
    HA = deepcopy(node.nodeReg[node_name])
    del node.nodeReg[node_name]

    state_root_hash = get_last_ordered_state_root_hash(node)
    node.master_replica._bls_bft_replica._bls_bft.bls_key_register._load_keys_for_root(state_root_hash)

    # Still we can validate Preprepare
    assert node.master_replica._bls_bft_replica._bls_bft.bls_key_register.get_key_by_name(node_name)

    node.nodeReg[node_name] = HA


def test_order_after_demote_and_restart(looper, txnPoolNodeSet,
                                        sdk_pool_handle, sdk_wallet_client, tdir, tconf, allPluginsPath,
                                        sdk_wallet_stewards):
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet,
                                         sdk_pool_handle, sdk_wallet_client, 3, 3)

    primary_node = txnPoolNodeSet[0]
    node_to_stop = txnPoolNodeSet[1]
    node_to_demote = txnPoolNodeSet[2]
    txnPoolNodeSet.remove(node_to_demote)

    node_to_stop.cleanupOnStopping = True
    node_to_stop.stop()
    looper.removeProdable(node_to_stop)
    ensure_node_disconnected(looper, node_to_stop, txnPoolNodeSet, timeout=2)

    demote_node(looper, sdk_wallet_stewards[2], sdk_pool_handle, node_to_demote)

    config_helper = PNodeConfigHelper(node_to_stop.name, tconf, chroot=tdir)
    restarted_node = TestNode(node_to_stop.name, config_helper=config_helper, config=tconf,
                              pluginPaths=allPluginsPath, ha=node_to_stop.nodestack.ha,
                              cliha=node_to_stop.clientstack.ha)
    looper.add(restarted_node)
    txnPoolNodeSet[1] = restarted_node
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet, check_primaries=False)

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet,
                                         sdk_pool_handle, sdk_wallet_client, 1, 1)

    def get_current_bls_keys(node):
        return node.master_replica._bls_bft_replica._bls_bft.bls_key_register._current_bls_keys

    assert get_current_bls_keys(restarted_node) == get_current_bls_keys(primary_node)


def get_last_ordered_state_root_hash(node):
    last_pre_prepare = \
        node.master_replica.prePrepares[node.master_replica.last_ordered_3pc]
    multi_sig = MultiSignature.from_list(*last_pre_prepare.blsMultiSig)
    state_root_hash = serializer.deserialize(multi_sig.value.pool_state_root_hash)
    return state_root_hash
