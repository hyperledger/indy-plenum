from plenum.test.helper import stopNodes, waitForViewChange, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, \
    reconnect_node_and_ensure_connected
from plenum.test.test_node import ensureElectionsDone, ensure_node_disconnected
from plenum.test.view_change.helper import ensure_view_change
from plenum.test.view_change.helper import start_stopped_node


# Do not remove these imports
from plenum.test.pool_transactions.conftest import client1, wallet1, client1Connected, looper

def stop_primary(looper, active_nodes):
    stopped_node = active_nodes[0]
    disconnect_node_and_ensure_disconnected(looper,
                                            active_nodes,
                                            stopped_node,
                                            stopNode=True)
    looper.removeProdable(stopped_node)
    active_nodes = active_nodes[1:]
    return stopped_node, active_nodes

def test_recover_stop_primaries(looper, txnPoolNodeSet, allPluginsPath, tconf, client1, wallet1, client1Connected):
    """
    Test that we can recover after having more than f nodes disconnected:
    - stop current master primary (Alpha)
    - send txns
    - restart current master primary (Beta)
    - send txns
    """

    active_nodes = list(txnPoolNodeSet)
    assert 4 == len(active_nodes)
    initial_view_no = active_nodes[0].viewNo

    # Stop first node (current Primary)
    _, active_nodes = stop_primary(looper, active_nodes)

    # Make sure view changed
    expected_view_no = initial_view_no + 1
    waitForViewChange(looper, active_nodes, expectedViewNo=expected_view_no)
    ensureElectionsDone(looper=looper, nodes=active_nodes, numInstances=2)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes)

    # send request
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, numReqs=10)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes)

    # Stop second node (current Primary)
    stopped_node, active_nodes = stop_primary(looper, active_nodes)

    # Restart second node
    restarted_node = start_stopped_node(stopped_node, looper, tconf, stopped_node.basedirpath, allPluginsPath)
    active_nodes = active_nodes + [restarted_node]

    # Check that primary selected
    ensureElectionsDone(looper=looper, nodes=active_nodes, numInstances=2, customTimeout=30)
    waitForViewChange(looper, active_nodes, expectedViewNo=expected_view_no)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, numReqs=10)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes)
