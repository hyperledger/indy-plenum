import pytest

from plenum.test.helper import stopNodes
from plenum.test.test_node import checkProtocolInstanceSetup, getRequiredInstances, \
    checkNodesConnected
from plenum.test import waits

VIEW_CHANGE_TIMEOUT = 10


def test_view_change_without_primary(txnPoolNodeSet, looper,
                                     patched_view_change_timeout):
    first, others = stop_nodes_and_remove_first(looper, txnPoolNodeSet)

    start_and_connect_nodes(looper, others)

    timeout = waits.expectedPoolElectionTimeout(len(txnPoolNodeSet)) + patched_view_change_timeout

    #looper.runFor(40)

    checkProtocolInstanceSetup(looper=looper, nodes=txnPoolNodeSet, retryWait=1,
                               customTimeout=timeout,
                               numInstances=getRequiredInstances(len(txnPoolNodeSet)))


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
def patched_view_change_timeout(txnPoolNodeSet):
    old_view_change_timeout = txnPoolNodeSet[0]._view_change_timeout
    for node in txnPoolNodeSet:
        node._view_change_timeout = VIEW_CHANGE_TIMEOUT
    yield VIEW_CHANGE_TIMEOUT
    for node in txnPoolNodeSet:
        node._view_change_timeout = old_view_change_timeout
