import pytest

from plenum.test import waits
from plenum.test.helper import checkViewNoForNodes, waitForViewChange, \
    sdk_send_random_and_check, sdk_send_batches_of_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, \
    reconnect_node_and_ensure_connected
from plenum.test.test_node import ensureElectionsDone, getRequiredInstances, \
    getNonPrimaryReplicas
from plenum.test.view_change.helper import ensure_view_change

nodeCount = 7
TestRunningTimeLimitSec = 150


@pytest.fixture(scope="module")
def tconf(tconf):
    old_chk = tconf.CHK_FREQ
    old_log = tconf.LOG_SIZE
    old_b_size = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1
    tconf.CHK_FREQ = 5
    tconf.LOG_SIZE = 5 * 3
    yield tconf
    tconf.CHK_FREQ = old_chk
    tconf.LOG_SIZE = old_log
    tconf.Max3PCBatchSize = old_b_size


def test_disconnected_node_with_lagged_view_pulls_up_its_view_on_reconnection(
        looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle, tconf):
    """
    Verifies that a disconnected node with a lagged view accepts
    the current view from the other nodes on re-connection.
    Steps:
    1. Provoke view change to 1.
    2. Ensure that all the nodes complete view change to 1.
    3. Disconnect one node from the rest of the nodes in the pool.
    4. Provoke view change to 2.
    5. Ensure that all the nodes except for the disconnected one complete
    view change to 2 and the disconnected node remains in the view 1.
    6. Provoke view change to 3.
    5. Ensure that all the nodes except for the disconnected one complete
    view change to 3 and the disconnected node remains in the view 1.
    8. Connect the disconnected node to the rest of the nodes in the pool.
    9. Ensure that the re-connected node completes view change to 3.
    10. Ensure that all the nodes participate in consensus.
    """
    checkViewNoForNodes(txnPoolNodeSet, 0)

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    checkViewNoForNodes(txnPoolNodeSet, 1)

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    lagged_node = getNonPrimaryReplicas(txnPoolNodeSet)[-1].node
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            lagged_node,
                                            stopNode=False)
    other_nodes = list(set(txnPoolNodeSet) - {lagged_node})

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    ensure_view_change(looper, other_nodes)
    ensureElectionsDone(looper, other_nodes,
                        instances_list=range(getRequiredInstances(len(txnPoolNodeSet))))
    ensure_all_nodes_have_same_data(looper, other_nodes)
    checkViewNoForNodes(other_nodes, 2)
    checkViewNoForNodes([lagged_node], 1)

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    ensure_view_change(looper, other_nodes)
    ensureElectionsDone(looper, other_nodes,
                        instances_list=range(getRequiredInstances(len(txnPoolNodeSet))))
    ensure_all_nodes_have_same_data(looper, other_nodes)
    checkViewNoForNodes(other_nodes, 3)
    checkViewNoForNodes([lagged_node], 1)

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    reconnect_node_and_ensure_connected(looper, txnPoolNodeSet, lagged_node)

    # The node can start view change, receive NEW_VIEW and start waiting for first ordered in the new view
    # But since the node is lagged for more than checkpoint, it can not do re-ordering, and has to wait until the
    # catchjup by checkpoints is started
    waitForViewChange(looper, [lagged_node], 3,
                      customTimeout=waits.expectedPoolElectionTimeout(
                          len(txnPoolNodeSet)))

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, num_reqs=2 * tconf.CHK_FREQ)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    checkViewNoForNodes(txnPoolNodeSet, 3)
    ensureElectionsDone(looper, txnPoolNodeSet)

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
