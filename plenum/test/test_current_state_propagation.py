from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node


def test_current_state_propagation(sdk_new_node_caught_up,
                                   txnPoolNodeSet,
                                   sdk_node_set_with_node_added_after_some_txns,
                                   tconf, tdir, allPluginsPath):
    """
    Checks that nodes send CurrentState to lagged nodes.
    """

    # 1. Start pool
    looper, new_node, _, _ = sdk_node_set_with_node_added_after_some_txns

    # 2. Stop one node
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            new_node,
                                            stopNode=True)
    looper.removeProdable(new_node)

    # 3. Start it again
    restarted_node = start_stopped_node(new_node, looper, tconf, tdir,
                                        allPluginsPath)
    txnPoolNodeSet[-1] = restarted_node
    looper.run(checkNodesConnected(txnPoolNodeSet))
    looper.runFor(5)

    # 4. Check that all nodes sent CurrentState
    for node in txnPoolNodeSet[:-1]:
        sent_times = node.spylog.count(
            node.send_current_state_to_lagging_node.__name__)
        assert sent_times != 0, "{} haven't sent CurrentState".format(node)
    looper.runFor(5)

    # 5. Check that it received CurrentState messages
    received_times = restarted_node.spylog.count(
        restarted_node.process_current_state_message.__name__)
    assert received_times != 0
