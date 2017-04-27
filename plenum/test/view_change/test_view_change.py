import types
from functools import partial

import pytest

from stp_core.loop.eventually import eventually
from plenum.server.node import Node
from plenum.test.delayers import delayNonPrimaries
from plenum.test.helper import waitForViewChange, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import getPrimaryReplica, ensureElectionsDone

nodeCount = 7


# noinspection PyIncorrectDocstring
@pytest.fixture()
def viewChangeDone(nodeSet, looper, up, wallet1, client1, viewNo):
    # Delay processing of PRE-PREPARE from all non primary replicas of master
    # so master's performance falls and view changes
    delayNonPrimaries(nodeSet, 0, 10)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    waitForViewChange(looper, nodeSet, expectedViewNo=viewNo+1)


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
    ensureElectionsDone(looper, nodeSet)
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
