from plenum.test.delayers import cDelay, icDelay
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules, start_delaying
from plenum.test.view_change.helper import add_new_node


def test_audit_multiple_uncommitted_node_regs(looper, tdir, tconf, allPluginsPath,
                                              txnPoolNodeSet,
                                              sdk_pool_handle,
                                              sdk_wallet_client,
                                              sdk_wallet_steward):
    '''
    - Delay COMMITS on 1 Node
    - Add 2 more nodes (so that the slow node hs multiple uncommitted node txns)
    - Make sure that all nodes have equal state eventually
    '''
    slow_node = txnPoolNodeSet[-1]
    fast_nodes = [node for node in txnPoolNodeSet if node != slow_node]
    slow_stashers = [slow_node.nodeIbStasher]

    # let's ignore view changes for simplicity here
    start_delaying([n.nodeIbStasher for n in txnPoolNodeSet], icDelay())

    with delay_rules(slow_stashers, cDelay()):
        # Add Node5
        new_node = add_new_node(looper,
                                fast_nodes,
                                sdk_pool_handle,
                                sdk_wallet_steward,
                                tdir,
                                tconf,
                                allPluginsPath,
                                name='Psi',
                                wait_till_added=False)
        txnPoolNodeSet.append(new_node)
        start_delaying(new_node.nodeIbStasher, icDelay())

        # Add Node6
        new_node = add_new_node(looper,
                                fast_nodes,
                                sdk_pool_handle,
                                sdk_wallet_steward,
                                tdir,
                                tconf,
                                allPluginsPath,
                                name='Eta',
                                wait_till_added=False)
        txnPoolNodeSet.append(new_node)
        start_delaying(new_node.nodeIbStasher, icDelay())

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=20)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
