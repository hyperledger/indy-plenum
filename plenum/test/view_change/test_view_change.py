import types

import pytest

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from stp_core.loop.eventually import eventually
from plenum.server.node import Node
from plenum.test.delayers import delayNonPrimaries, \
    reset_delays_and_process_delayeds
from plenum.test.helper import waitForViewChange, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import getPrimaryReplica, get_master_primary_node, \
    ensureElectionsDone, checkProtocolInstanceSetup
from plenum.test.test_node import getPrimaryReplica, ensureElectionsDone

nodeCount = 7


# noinspection PyIncorrectDocstring
@pytest.fixture()
def viewChangeDone(nodeSet, looper, up, wallet1, client1, viewNo):
    m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    # Delay processing of PRE-PREPARE from all non primary replicas of master
    # so master's performance falls and view changes
    npr = delayNonPrimaries(nodeSet, 0, 10)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)
    waitForViewChange(looper, nodeSet, expectedViewNo=viewNo+1)
    reset_delays_and_process_delayeds([r.node for r in npr])

    ensure_all_nodes_have_same_data(looper, nodeSet)
    # ensureElectionsDone(looper=looper, nodes=nodeSet)
    checkProtocolInstanceSetup(looper, nodeSet)
    new_m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    assert m_primary_node.name != new_m_primary_node.name


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

def testViewChangeTimeout(nodeSet, looper, up, wallet1, client1, viewNo):
    # TODO: this probably should be moved to other file

    from plenum.test.delayers import icDelay

    for node in nodeSet:
        node._primary_election_timeout = 5
        old = node._check_view_change_completed

        def new(*args, **kwargs):
            print("CALLED!")
            old(*args)

        node._check_view_change_completed = new
        # reset_delays_and_process_delayeds(node)

    for node in nodeSet:
        node.nodeIbStasher.delay(icDelay(delay=5))

    m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    # Delay processing of PRE-PREPARE from all non primary replicas of master
    # so master's performance falls and view changes
    delayNonPrimaries(nodeSet, 0, 10)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    ensure_all_nodes_have_same_data(looper, nodeSet)
    checkProtocolInstanceSetup(looper, nodeSet)
    new_m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    assert not m_primary_node.name != new_m_primary_node.name


def test_node_notified_about_primary_election_result(nodeSet, looper, up, wallet1, client1, viewNo):
    primary_notified = False
    for node in nodeSet:
        old = node.primary_found
        def new(*args, **kwargs):
            nonlocal primary_notified
            primary_notified = True
            old()
        node.primary_found = new
    delayNonPrimaries(nodeSet, 0, 10)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)
    waitForViewChange(looper, nodeSet, expectedViewNo=viewNo + 1)
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    assert primary_notified
