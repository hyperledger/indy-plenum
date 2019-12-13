from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data

from plenum.test.node_catchup.test_config_ledger import start_stopped_node

from plenum.test.helper import sdk_send_random_and_check, checkViewNoForNodes, waitForViewChange
from plenum.test.pool_transactions.helper import demote_node, disconnect_node_and_ensure_disconnected, promote_node
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected

nodeCount = 7


def test_promotion_before_view_change(looper,
                                      txnPoolNodeSet,
                                      tdir,
                                      tconf,
                                      allPluginsPath,
                                      sdk_wallet_stewards,
                                      sdk_pool_handle):

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards[0], 1)
    assert txnPoolNodeSet[0].master_replica.isPrimary
    assert txnPoolNodeSet[1].replicas[1].isPrimary
    assert txnPoolNodeSet[2].replicas[2].isPrimary
    starting_view_number = checkViewNoForNodes(txnPoolNodeSet)

    node_2 = txnPoolNodeSet[1]
    node_3 = txnPoolNodeSet[2]
    node_5 = txnPoolNodeSet[4]

    # Demote node 2
    steward_2 = sdk_wallet_stewards[1]
    demote_node(looper, steward_2, sdk_pool_handle, node_2)
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, node_2)
    looper.removeProdable(node_2)
    txnPoolNodeSet.remove(node_2)

    # Checking that view change happened
    # we are expecting 2 view changes here since Beta is selected as a master Primary on view=1
    # (since node reg at the beginning of view 0 is used to select it), but it's not available (demoted),
    # so we do view change to view=2 by timeout
    waitForViewChange(looper,
                      txnPoolNodeSet,
                      expectedViewNo=starting_view_number + 2)
    ensureElectionsDone(looper, txnPoolNodeSet, instances_list=[0, 1])
    assert node_3.master_replica.isPrimary

    # Promoting node 3, increasing replica count
    node_2 = start_stopped_node(node_2, looper, tconf, tdir, allPluginsPath)
    promote_node(looper, steward_2, sdk_pool_handle, node_2)
    txnPoolNodeSet.append(node_2)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitForViewChange(looper,
                      txnPoolNodeSet,
                      expectedViewNo=starting_view_number + 3)
    ensureElectionsDone(looper, txnPoolNodeSet, instances_list=[0, 1, 2])
    # node 5 is a primary since promoted node is added at the end of the list
    assert node_5.master_replica.isPrimary

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards[0], 2)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
