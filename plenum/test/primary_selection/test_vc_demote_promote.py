from plenum.test.helper import sdk_send_random_and_check, checkViewNoForNodes, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_catchup.test_config_ledger import start_stopped_node
from plenum.test.pool_transactions.helper import demote_node, disconnect_node_and_ensure_disconnected, promote_node
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected
from plenum.test.view_change.helper import ensure_view_change_complete

nodeCount = 7


def test_vc_demote_last_and_promote(looper,
                                    txnPoolNodeSet,
                                    tdir,
                                    tconf,
                                    allPluginsPath,
                                    sdk_wallet_stewards,
                                    sdk_pool_handle):
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards[0], 5)

    # do view change
    ensure_view_change_complete(looper, txnPoolNodeSet)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards[0], 5)
    starting_view_number = checkViewNoForNodes(txnPoolNodeSet)
    assert starting_view_number == 1

    # Demote last node
    last_node = txnPoolNodeSet[-1]
    last_steward = sdk_wallet_stewards[-1]
    demote_node(looper, last_steward, sdk_pool_handle, last_node)
    txnPoolNodeSet.remove(last_node)

    # Checking that view change happened
    waitForViewChange(looper,
                      txnPoolNodeSet,
                      expectedViewNo=starting_view_number + 1)
    ensureElectionsDone(looper, txnPoolNodeSet, instances_list=[0, 1])

    # send more txns
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards[0], 5)

    # Promoting last node, increasing replica count
    promote_node(looper, last_steward, sdk_pool_handle, last_node)

    # Restart promoted node
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, last_node)
    looper.removeProdable(last_node)
    last_node = start_stopped_node(last_node, looper, tconf, tdir, allPluginsPath)
    txnPoolNodeSet.append(last_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Checking that view change happened
    waitForViewChange(looper,
                      txnPoolNodeSet,
                      expectedViewNo=starting_view_number + 2)
    ensureElectionsDone(looper, txnPoolNodeSet, instances_list=[0, 1, 2])

    # ensure pool functional
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards[0], 5)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
