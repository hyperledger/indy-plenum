import types

import pytest

from plenum.server.view_change.view_changer import ViewChanger
from plenum.test.delayers import delayNonPrimaries
from plenum.test.helper import waitForViewChange, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import get_master_primary_node, getPrimaryReplica, \
    ensureElectionsDone
from plenum.test.view_change.helper import simulate_slow_master

nodeCount = 7

# noinspection PyIncorrectDocstring


def test_view_change_on_performance_degraded(looper, nodeSet, up, viewNo,
                                             wallet1, client1):
    """
    Test that a view change is done when the performance of master goes down
    Send multiple requests from the client and delay some requests by master
    instance so that there is a view change. All nodes will agree that master
    performance degraded
    """
    old_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))

    simulate_slow_master(looper, nodeSet, wallet1, client1)
    waitForViewChange(looper, nodeSet, expectedViewNo=viewNo + 1)

    ensureElectionsDone(looper=looper, nodes=nodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)
    new_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    assert old_primary_node.name != new_primary_node.name


def test_view_change_on_quorum_of_master_degraded(nodeSet, looper, up,
                                                  wallet1, client1, viewNo):
    """
    Node will change view even though it does not find the master to be degraded
    when a quorum of nodes agree that master performance degraded
    """

    m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))

    # Delay processing of PRE-PREPARE from all non primary replicas of master
    # so master's performance falls and view changes
    delayNonPrimaries(nodeSet, 0, 10)

    pr = getPrimaryReplica(nodeSet, 0)
    relucatantNode = pr.node

    # Count sent instance changes of all nodes
    sentInstChanges = {}
    instChngMethodName = ViewChanger.sendInstanceChange.__name__
    for n in nodeSet:
        sentInstChanges[n.name] = n.view_changer.spylog.count(instChngMethodName)

    # Node reluctant to change view, never says master is degraded
    relucatantNode.monitor.isMasterDegraded = types.MethodType(
        lambda x: False, relucatantNode.monitor)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    # Check that view change happened for all nodes
    waitForViewChange(looper, nodeSet, expectedViewNo=viewNo + 1)

    # All nodes except the reluctant node should have sent a view change and
    # thus must have called `sendInstanceChange`
    for n in nodeSet:
        if n.name != relucatantNode.name:
            assert n.view_changer.spylog.count(instChngMethodName) > \
                sentInstChanges.get(n.name, 0)
        else:
            assert n.view_changer.spylog.count(instChngMethodName) == \
                sentInstChanges.get(n.name, 0)

    ensureElectionsDone(looper=looper, nodes=nodeSet)
    new_m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    assert m_primary_node.name != new_m_primary_node.name
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)
