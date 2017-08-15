from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.spy_helpers import get_count
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change

nodeCount = 7


# noinspection PyIncorrectDocstring
def test_view_change_on_empty_ledger(nodeSet, up, looper):
    """
    Check that view change is done when no txns in the ldegr
    """
    ensure_view_change(looper, nodeSet)
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)


# noinspection PyIncorrectDocstring
def test_view_change_after_some_txns(looper, nodeSet, up, viewNo,
                                     wallet1, client1):
    """
    Check that view change is done after processing some of txns
    """
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    ensure_view_change(looper, nodeSet)
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)


# noinspection PyIncorrectDocstring
def test_send_more_after_view_change(looper, nodeSet, up,
                                     wallet1, client1):
    """
    Check that we can send more requests after view change
    """
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    ensure_view_change(looper, nodeSet)
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 10)


def test_node_notified_about_primary_election_result(nodeSet, looper, up):
    old_counts = {node.name: get_count(
        node, node.primary_selected) for node in nodeSet}
    ensure_view_change(looper, nodeSet)
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)

    for node in nodeSet:
        assert get_count(node, node.primary_selected) > old_counts[node.name]
