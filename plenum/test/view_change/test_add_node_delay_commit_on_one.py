from plenum.test.delayers import cDelay
from plenum.test.helper import waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.stasher import delay_rules
from plenum.test.test_node import checkNodesConnected, ensureElectionsDone


def test_add_node_delay_commit_on_one(looper, txnPoolNodeSet, sdk_pool_handle,
                                      sdk_wallet_steward, tdir, tconf, allPluginsPath):
    view_no = txnPoolNodeSet[-1].viewNo
    # Add a New node but don't allow Delta to be aware of it. We do not want it in Delta's node registry.
    with delay_rules(txnPoolNodeSet[-1].nodeIbStasher, cDelay()):
        _, new_node = sdk_add_new_steward_and_node(looper, sdk_pool_handle, sdk_wallet_steward,
                                                   'New_Steward', 'Epsilon',
                                                   tdir, tconf, allPluginsPath=allPluginsPath)
        txnPoolNodeSet.append(new_node)
        looper.run(checkNodesConnected(txnPoolNodeSet[:-2] + [new_node]))
        waitForViewChange(looper, txnPoolNodeSet, view_no + 1)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
