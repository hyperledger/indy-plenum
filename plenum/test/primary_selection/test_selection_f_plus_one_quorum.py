from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, \
    reconnect_node_and_ensure_connected
from plenum.test.test_node import ensureElectionsDone
from plenum.test.helper import stopNodes
from plenum.test.view_change.helper import ensure_view_change

# Do not remove these imports
from plenum.test.conftest import nodeSet, up, looper


def test_selection_f_plus_one_quorum(nodeSet, up, looper):
    """
    Check that quorum f + 1 is used for primary selection 
    when initiated by CurrentState messages.

    Assumes that view change quorum is n - f.
    Assumes that primaries selection in round robin fashion.
    """

    # Ensure that we have 4 nodes in total
    assert len(nodeSet) == 4
    all_nodes = list(nodeSet)
    alpha, beta, gamma, delta = all_nodes

    # Make one node lagging by switching it off for some time
    lagging_node = delta
    non_lagging_nodes = [alpha, beta, gamma]
    disconnect_node_and_ensure_disconnected(looper,
                                            all_nodes,
                                            lagging_node,
                                            stopNode=True)
    looper.removeProdable(lagging_node)

    # Make nodes to perform view change
    ensure_view_change(looper, non_lagging_nodes)
    ensureElectionsDone(looper=looper, nodes=non_lagging_nodes, numInstances=2)
    ensure_all_nodes_have_same_data(looper, nodes=non_lagging_nodes)

    # Stop two more of active nodes
    # (but not primary, which is Beta (because of round robin selection))
    stopped_nodes = [alpha, gamma]
    stopNodes(stopped_nodes, looper)
    looper.removeProdable(*stopped_nodes)

    # Start lagging node back
    active_nodes = [beta, lagging_node]
    looper.add(lagging_node)
    reconnect_node_and_ensure_connected(looper, active_nodes, lagging_node)
    looper.runFor(5)

    # Check that primary selected
    ensureElectionsDone(looper=looper, nodes=active_nodes, numInstances=2)
