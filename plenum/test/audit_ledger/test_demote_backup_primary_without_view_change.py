from plenum.test.delayers import icDelay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import demote_node, disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node

nodeCount = 6
whitelist = ['Audit ledger has inconsistent names of primaries', ]


def test_demote_backup_primary_without_view_change(looper, txnPoolNodeSet, sdk_pool_handle,
                                                   sdk_wallet_stewards, tdir, tconf, allPluginsPath):
    assert len(txnPoolNodeSet) > 4

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_stewards[0], 1)

    lagging_instance = 1
    node_to_restart = txnPoolNodeSet[-1]
    node_to_demote = steward_for_demote_node = demote_node_index = None
    steward_for_demote_node = None
    for i, n in enumerate(txnPoolNodeSet):
        if n.name == txnPoolNodeSet[0].primaries[lagging_instance]:
            node_to_demote = n
            steward_for_demote_node = sdk_wallet_stewards[i]
            demote_node_index = i
            break
    assert node_to_demote

    with delay_rules([n.nodeIbStasher
                      for n in txnPoolNodeSet
                      if n != txnPoolNodeSet[demote_node_index]],
                     icDelay()):
        demote_node(looper, steward_for_demote_node, sdk_pool_handle,
                    node_to_demote)
        del txnPoolNodeSet[demote_node_index]

        ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

        disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, node_to_restart)
        looper.removeProdable(name=node_to_restart.name)
        node_to_restart = start_stopped_node(node_to_restart, looper, tconf,
                                             tdir, allPluginsPath)
        txnPoolNodeSet[-1] = node_to_restart
        looper.run(checkNodesConnected(txnPoolNodeSet))
        ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_stewards[0], 1)
        ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
        for instance_id, r in node_to_restart.replicas.items():
            if instance_id == lagging_instance:
                continue
            assert r.last_ordered_3pc == node_to_restart.master_last_ordered_3PC
