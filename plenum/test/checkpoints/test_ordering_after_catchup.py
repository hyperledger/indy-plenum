from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    checkViewNoForNodes
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    ensure_all_nodes_have_same_data
from plenum.common.util import randomString
from plenum.test.test_node import checkNodesConnected
from plenum.test.pool_transactions.helper import addNewStewardAndNode

CHK_FREQ = 6
LOG_SIZE = 3 * CHK_FREQ

nodeCount = 4


def add_new_node(looper, pool_nodes, steward, steward_wallet,
                 tdir, client_tdir, tconf, all_plugins_path):

    name = randomString(6)
    node_name = "Node-" + name
    new_steward_name = "Steward-" + name

    _, _, new_node = addNewStewardAndNode(looper, steward, steward_wallet,
                                          new_steward_name, node_name,
                                          tdir, client_tdir, tconf,
                                          all_plugins_path)
    pool_nodes.append(new_node)
    looper.run(checkNodesConnected(pool_nodes))
    waitNodeDataEquality(looper, new_node, *pool_nodes[:-1])
    # The new node did not participate in ordering of the batch with
    # the new steward NYM transaction and the batch with the new NODE
    # transaction. The new node got these transactions via catch-up.

    return new_node


def test_ordering_after_more_than_f_nodes_caught_up(
        chkFreqPatched, looper, txnPoolNodeSet, steward1, stewardWallet,
        tdir, client_tdir, tconf, allPluginsPath):
    """
    Verifies that more than LOG_SIZE batches can be ordered in one view
    after more than f nodes caught up in this view when some 3PC-batches
    had already been ordered in this view.
    """
    initial_view_no = txnPoolNodeSet[0].viewNo

    for _ in range(2):
        add_new_node(looper, txnPoolNodeSet, steward1, stewardWallet,
                     tdir, client_tdir, tconf, allPluginsPath)
    checkViewNoForNodes(txnPoolNodeSet, initial_view_no)

    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, 20)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    checkViewNoForNodes(txnPoolNodeSet, initial_view_no)
