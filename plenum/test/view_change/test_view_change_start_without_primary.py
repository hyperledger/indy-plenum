import pytest

from plenum.test.helper import stopNodes
from plenum.test.test_node import checkProtocolInstanceSetup, getRequiredInstances, \
    checkNodesConnected
from plenum.test import waits


def test_view_change_without_primary(txnPoolNodeSet, looper, tconf):
    first, others = stop_nodes_and_remove_first(looper, txnPoolNodeSet)

    start_and_connect_nodes(looper, others)

    timeout = waits.expectedPoolElectionTimeout(len(txnPoolNodeSet)) + tconf.NEW_VIEW_TIMEOUT

    # looper.runFor(40)

    checkProtocolInstanceSetup(looper=looper, nodes=txnPoolNodeSet, retryWait=1,
                               customTimeout=timeout,
                               instances=range(getRequiredInstances(len(txnPoolNodeSet))))


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
