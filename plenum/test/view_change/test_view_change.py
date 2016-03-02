from functools import partial

import pytest
from plenum.test.eventually import eventually

from plenum.test.helper import getNonPrimaryReplicas, ppDelay, \
    checkViewNoForNodes, sendReqsToNodesAndVerifySuffReplies

nodeCount = 7


# noinspection PyIncorrectDocstring
@pytest.fixture()
def viewChangeDone(nodeSet, looper, up, client1):
    """
    Test that a view change is done when the performance of master goes down
    """

    # Delay processing of PRE-PREPARE from all non primary replicas of master
    # so master's performance falls and view changes
    nonPrimReps = getNonPrimaryReplicas(nodeSet, 0)
    for r in nonPrimReps:
        r.node.nodeIbStasher.delay(ppDelay(10, 0))

    sendReqsToNodesAndVerifySuffReplies(looper, client1, 4)

    looper.run(eventually(partial(checkViewNoForNodes, nodeSet, 1),
                          retryWait=1, timeout=20))


# noinspection PyIncorrectDocstring
def testViewChange(viewChangeDone):
    """
    Send multiple requests from the client and delay some requests by master
    instance so that there is a view change
    """
    pass
