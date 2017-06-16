import types

import pytest

from plenum.server.node import Node
from plenum.test.delayers import delayNonPrimaries, \
    reset_delays_and_process_delayeds
from plenum.test.helper import waitForViewChange, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import get_master_primary_node, getPrimaryReplica, \
    ensureElectionsDone
from plenum.test.delayers import icDelay
from plenum.test.view_change.helper import ensure_view_change

nodeCount = 7


@pytest.fixture()
def viewChangeDone(simulate_slow_master):
    primary_node = simulate_slow_master()
    assert primary_node.old.name != primary_node.new.name


# noinspection PyIncorrectDocstring
def testViewChange(viewChangeDone):
    """
    Test that a view change is done when the performance of master goes down
    Send multiple requests from the client and delay some requests by master
    instance so that there is a view change. All nodes will agree that master
    performance degraded
    """
    pass


def testViewChangeCase1(nodeSet, looper, up, wallet1, client1, viewNo):
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
    instChngMethodName = Node.sendInstanceChange.__name__
    for n in nodeSet:
        sentInstChanges[n.name] = n.spylog.count(instChngMethodName)

    # Node reluctant to change view, never says master is degraded
    relucatantNode.monitor.isMasterDegraded = types.MethodType(
        lambda x: False, relucatantNode.monitor)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    # Check that view change happened for all nodes
    waitForViewChange(looper, nodeSet, expectedViewNo=viewNo+1)

    # All nodes except the reluctant node should have sent a view change and
    # thus must have called `sendInstanceChange`
    for n in nodeSet:
        if n.name != relucatantNode.name:
            assert n.spylog.count(instChngMethodName) > \
                   sentInstChanges.get(n.name, 0)
        else:
            assert n.spylog.count(instChngMethodName) == \
                   sentInstChanges.get(n.name, 0)

    ensureElectionsDone(looper=looper, nodes=nodeSet)
    new_m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    assert m_primary_node.name != new_m_primary_node.name


def test_view_change_timeout(nodeSet, looper, up, wallet1, client1, viewNo):
    """
    Check view change restarted if it is not completed in time
    """

    m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))

    # Setting view change timeout to low value to make test pass quicker
    for node in nodeSet:
        node._view_change_timeout = 5

    # Delaying view change messages to make first view change fail
    # due to timeout
    for node in nodeSet:
        node.nodeIbStasher.delay(icDelay(delay=5))

    # Delaying preprepae messages from nodes and
    # sending request to force view change
    for i in range(3):
        delayNonPrimaries(nodeSet, instId=i, delay=10)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    # First view change should fail, because of delayed
    # instance change messages.
    # This then leads to new view change that we need.
    try:
        ensure_view_change(looper, nodeSet)
        ensureElectionsDone(looper=looper, nodes=nodeSet)
    except AssertionError:
        pass

    # Resetting delays to let second view change go well
    reset_delays_and_process_delayeds(nodeSet)

    # This view change should be completed with no problems
    ensure_view_change(looper, nodeSet)
    ensureElectionsDone(looper=looper, nodes=nodeSet)

    new_m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    assert m_primary_node.name != new_m_primary_node.name
    for node in nodeSet:
        assert node.spylog.count('_check_view_change_completed') > 0
    for node in nodeSet:
        assert node.viewNo == (viewNo + 2)


def test_node_notified_about_primary_election_result(nodeSet, looper, up, wallet1, client1, viewNo):
    delayNonPrimaries(nodeSet, 0, 10)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)
    waitForViewChange(looper, nodeSet, expectedViewNo=viewNo + 1)
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    for node in nodeSet:
        assert node.spylog.count('primary_found') > 0
