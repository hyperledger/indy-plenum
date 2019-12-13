from plenum.test.delayers import cDelay, ppDelay
from plenum.test.helper import waitForViewChange, checkViewNoForNodes
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import ensure_view_change_complete

nodeCount = 7


def test_nemanja(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, tdir, tconf, allPluginsPath):

    view_no = txnPoolNodeSet[-1].viewNo

    # Force 5 view changes so that we have viewNo == 5 and Zeta as the primary.
    for _ in range(5):
        ensure_view_change_complete(looper, txnPoolNodeSet)
        view_no = checkViewNoForNodes(txnPoolNodeSet)

    # Add a New node but don't allow Alpha to be aware it. We do not want it in Alpha's node registry.
    with delay_rules_without_processing(txnPoolNodeSet[0].nodeIbStasher, ppDelay(), ppDelay(), cDelay()):
        _, new_node = sdk_add_new_steward_and_node(looper, sdk_pool_handle, sdk_wallet_steward, 'New_Steward', 'New',
                                                   tdir, tconf, allPluginsPath=allPluginsPath)
        txnPoolNodeSet.append(new_node)

        nodes_sans_alpha = txnPoolNodeSet[1:]
        looper.run(checkNodesConnected(nodes_sans_alpha))

    # All node performed another VIEW_CHANGE so we should have viewNo == 6 and Eta as the primary.
    # Next in line to be the primary is New, and all nodes except Alpha have the New node in node registry.
    waitForViewChange(looper, txnPoolNodeSet, view_no + 1)

    ensure_view_change_complete(looper, txnPoolNodeSet)
    assert 1 == 1
