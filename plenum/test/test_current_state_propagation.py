from plenum.test.node_catchup.conftest import whitelist, \
    looper, nodeCreatedAfterSomeTxns, nodeSetWithNodeAddedAfterSomeTxns, \
    newNodeCaughtUp
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, reconnect_node_and_ensure_connected
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    waitNodeDataInequality, checkNodeDataForEquality
from plenum.test.pool_transactions.conftest import stewardAndWallet1, \
    steward1, stewardWallet, clientAndWallet1, client1, wallet1, \
    client1Connected


def test_current_state_propagation(newNodeCaughtUp,
                                   txnPoolNodeSet,
                                   nodeSetWithNodeAddedAfterSomeTxns):
    """
    Checks that nodes send CurrentState to lagged nodes.
    """

    # 1. Start pool
    looper, new_node, client, wallet, _, _ = nodeSetWithNodeAddedAfterSomeTxns

    # 2. Stop one node
    lagging_node = new_node
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            lagging_node,
                                            stopNode=True)
    looper.removeProdable(new_node)

    # 3. Start it again
    looper.add(new_node)
    reconnect_node_and_ensure_connected(looper, txnPoolNodeSet, new_node)
    looper.runFor(5)

    # 4. Check that all nodes sent CurrentState
    for node in txnPoolNodeSet[:-1]:
        sent_times = node.spylog.count(
            node.send_current_state_to_lagging_node.__name__)
        assert sent_times != 0, "{} haven't sent CurrentState".format(node)
    looper.runFor(5)

    # 5. Check that it received CurrentState messages
    received_times = lagging_node.spylog.count(
        lagging_node.process_current_state_message.__name__)
    assert received_times != 0
