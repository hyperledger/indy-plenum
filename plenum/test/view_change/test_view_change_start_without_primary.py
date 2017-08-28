import pytest

from plenum.test.helper import stopNodes
from plenum.test.test_node import checkProtocolInstanceSetup, getRequiredInstances, \
    checkNodesConnected
from plenum.test import waits

view_change_timeout = 10


def test_view_change_without_primary(nodeSet, looper,
                                     patched_view_change_timeout):

    first, others = stop_nodes_and_remove_first(looper, nodeSet)

    start_and_connect_nodes(looper, others)

    timeout = waits.expectedPoolElectionTimeout(len(nodeSet)) + patched_view_change_timeout

    checkProtocolInstanceSetup(looper=looper, nodes=others, retryWait=1,
                               customTimeout=timeout,
                               numInstances=getRequiredInstances(len(nodeSet)))


def stop_nodes_and_remove_first(looper, nodes):
    first_node = nodes[0]
    stopNodes(nodes, looper)
    looper.removeProdable(first_node)
    looper.runFor(3)  # let the nodes stop
    return first_node, \
        list(filter(lambda x: x.name != first_node.name, nodes))


def start_and_connect_nodes(looper, nodes):
    for n in nodes:
        n.start(looper.loop)
    looper.run(checkNodesConnected(nodes))


@pytest.fixture(scope='function')
def patched_view_change_timeout(nodeSet):
    old_view_change_timeout = nodeSet[0]._view_change_timeout
    for node in nodeSet:
        node._view_change_timeout = view_change_timeout
    yield view_change_timeout
    for node in nodeSet:
        node._view_change_timeout = old_view_change_timeout
