from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data

from plenum.test.test_node import get_master_primary_node, ensure_node_disconnected
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check, waitForViewChange


def test_view_changes_if_master_primary_disconnected(txnPoolNodeSet, looper, sdk_pool_handle,
                                                     sdk_wallet_client, tdir, tconf, allPluginsPath):
    """
    View change occurs when master's primary is disconnected
    """

    # Setup
    nodes = txnPoolNodeSet

    old_view_no = checkViewNoForNodes(nodes)
    old_pr_node = get_master_primary_node(nodes)

    # Stop primary
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet,
                                            old_pr_node, stopNode=True)

    remaining_nodes = list(set(nodes) - {old_pr_node})
    # Sometimes it takes time for nodes to detect disconnection
    ensure_node_disconnected(looper, old_pr_node, remaining_nodes, timeout=20)

    looper.runFor(tconf.ToleratePrimaryDisconnection + 2)

    # Give some time to detect disconnection and then verify that view has
    # changed and new primary has been elected
    waitForViewChange(looper, remaining_nodes, old_view_no + 1)
    ensure_all_nodes_have_same_data(looper, nodes=remaining_nodes)
    new_pr_node = get_master_primary_node(remaining_nodes)
    assert old_pr_node != new_pr_node

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)

    # Check if old primary can join the pool and still functions
    old_pr_node = start_stopped_node(old_pr_node, looper, tconf,
                                     tdir, allPluginsPath)

    txnPoolNodeSet = remaining_nodes + [old_pr_node]
    looper.run(eventually(checkViewNoForNodes,
                          txnPoolNodeSet, old_view_no + 1, timeout=tconf.VIEW_CHANGE_TIMEOUT))
    assert len(getAllReturnVals(old_pr_node.view_changer,
                                old_pr_node.view_changer._start_view_change_if_possible,
                                compare_val_to=True)) > 0

    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    assert not old_pr_node.view_changer._next_view_indications
