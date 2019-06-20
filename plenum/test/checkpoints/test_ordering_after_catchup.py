from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    ensure_all_nodes_have_same_data
from plenum.common.util import randomString
from plenum.test.test_node import checkNodesConnected
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node

CHK_FREQ = 6
LOG_SIZE = 3 * CHK_FREQ

nodeCount = 4


def add_new_node(looper, pool_nodes, sdk_pool_handle, sdk_wallet_steward,
                 tdir, tconf, all_plugins_path):
    name = randomString(6)
    node_name = "Node-" + name
    new_steward_name = "Steward-" + name

    _, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        new_steward_name, node_name, tdir, tconf,
        allPluginsPath=all_plugins_path)
    pool_nodes.append(new_node)
    looper.run(checkNodesConnected(pool_nodes))
    waitNodeDataEquality(looper, new_node, *pool_nodes[:-1], exclude_from_check=['check_last_ordered_3pc_backup'])
    # The new node did not participate in ordering of the batch with
    # the new steward NYM transaction and the batch with the new NODE
    # transaction. The new node got these transactions via catch-up.

    return new_node


def test_ordering_after_more_than_f_nodes_caught_up(
        chkFreqPatched, looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward,
        tdir, tconf, allPluginsPath):
    """
    Verifies that more than LOG_SIZE batches can be ordered in one view
    after more than f nodes caught up in this view when some 3PC-batches
    had already been ordered in this view.
    """
    initial_view_no = txnPoolNodeSet[0].viewNo

    for _ in range(2):
        add_new_node(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward,
                     tdir, tconf, allPluginsPath)
    checkViewNoForNodes(txnPoolNodeSet, initial_view_no)

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 20)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, exclude_from_check=['check_last_ordered_3pc_backup'])
    checkViewNoForNodes(txnPoolNodeSet, initial_view_no)
