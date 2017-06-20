import pytest
from plenum.test.delayers import delay_3pc_messages, reset_delays_and_process_delayeds
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, sendRandomRequests
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change

nodeCount = 7


def view_change_in_between_3pc(looper, nodes, wallet, client,
                               slow_nodes_count, slow_delay=1, wait=None):
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 4)

    slow_nodes = list(nodes)[:slow_nodes_count]
    delay_3pc_messages(slow_nodes, 0, delay=slow_delay)

    sendRandomRequests(wallet, client, 10)
    if wait:
        looper.runFor(wait)

    ensure_view_change(looper, nodes)
    ensureElectionsDone(looper=looper, nodes=nodes)
    ensure_all_nodes_have_same_data(looper, nodes=nodes)

    reset_delays_and_process_delayeds(slow_nodes)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 2)

def view_change_in_between_3pc_random_delays(looper, nodes, wallet, client, slow_nodes_count,
                                             min_delay=0, max_delay=5):
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 4)

    slow_nodes = list(nodes)[:slow_nodes_count]
    delay_3pc_messages(slow_nodes, 0, min_delay=min_delay, max_delay=max_delay)

    sendRandomRequests(wallet, client, 10)

    ensure_view_change(looper, nodes)
    ensureElectionsDone(looper=looper, nodes=nodes)
    ensure_all_nodes_have_same_data(looper, nodes=nodes)

    reset_delays_and_process_delayeds(slow_nodes)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 10)




# noinspection PyIncorrectDocstring
def test_view_change_in_between_3pc_1_of_7_nodes(looper, nodeSet, up,
                                                 wallet1, client1):
    """
    - Slow processing 3PC messages for 1 of 7 node
    - do view change
    """
    view_change_in_between_3pc(looper, nodeSet, wallet1, client1, 1)

@pytest.mark.skip
def test_view_change_in_between_3pc_2_of_7_nodes(looper, nodeSet, up,
                                                 wallet1, client1):
    """
    - Slow processing 3PC messages for 2 of 7 node (2=f)
    - do view change
    """
    view_change_in_between_3pc(looper, nodeSet, wallet1, client1, 2)

@pytest.mark.skip
def test_view_change_in_between_3pc_3_of_7_nodes(looper, nodeSet, up,
                                                 wallet1, client1):
    """
    - Slow processing 3PC messages for 3 of 7 node (3>f)
    - do view change
    """
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 3)

@pytest.mark.skip
def test_view_change_in_between_3pc_5_of_7_nodes(looper, nodeSet, up,
                                                 wallet1, client1):
    """
    - Slow processing 3PC messages for 5 of 7 node (5=2f+1)
    - do view change
    """
    view_change_in_between_3pc(looper, nodeSet, wallet1, client1, 5)

@pytest.mark.skip
def test_view_change_in_between_3pc_all_nodes(looper, nodeSet, up,
                                              wallet1, client1):
    """
    - Slow processing 3PC messages for all nodes
    - do view change
    """
    view_change_in_between_3pc(looper, nodeSet, wallet1, client1, len(nodeSet))

@pytest.mark.skip
def test_view_change_in_between_3pc_all_nodes_random_delays(looper, nodeSet, up,
                                              wallet1, client1):
    """
    - Slow processing 3PC messages for all nodes
    - do view change
    """
    view_change_in_between_3pc_random_delays(looper, nodeSet, wallet1, client1, len(nodeSet))