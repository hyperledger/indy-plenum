import logging
from functools import partial

import pytest
from zeno.test.eventually import eventually

from zeno.common.util import getMaxFailures
from zeno.test.helper import TestReplica
from zeno.test.helper import getNonPrimaryReplicas, sendRandomRequest, \
    ppDelay, checkViewChangeInitiatedForNode, icDelay, \
    sendReqsToNodesAndVerifySuffReplies

nodeCount = 7


# noinspection PyIncorrectDocstring
def testQueueingReqFromFutureView(delayedPerf, looper, nodeSet, up, client1):
    """
    Test if every node queues 3 Phase requests(PRE-PREPARE, PREPARE and COMMIT)
    that come from a view which is greater than the current view
    """

    f = getMaxFailures(nodeCount)

    # Delay processing of instance change on a node
    nodeA = nodeSet.Alpha
    nodeA.nodeIbStasher.delay(icDelay(60))

    nonPrimReps = getNonPrimaryReplicas(nodeSet, 0)
    # Delay processing of PRE-PREPARE from all non primary replicas of master
    # so master's throughput falls and view changes
    ppDelayer = ppDelay(5, 0)
    for r in nonPrimReps:
        r.node.nodeIbStasher.delay(ppDelayer)

    sendReqsToNodesAndVerifySuffReplies(looper, client1, 4,
                                        timeout=5 * nodeCount)

    # Every node except Node A should have a view change
    for node in nodeSet:
        if node.name != nodeA.name:
            looper.run(eventually(
                partial(checkViewChangeInitiatedForNode, node, 0),
                retryWait=1,
                timeout=20))

    # Node A's view should not have changed yet
    with pytest.raises(AssertionError):
        looper.run(eventually(partial(
            checkViewChangeInitiatedForNode, nodeA, 0),
            retryWait=1,
            timeout=20))

    # NodeA should not have any pending 3 phase request for a later view
    for r in nodeA.replicas:  # type: TestReplica
        assert len(r.threePhaseMsgsForLaterView) == 0

    # Reset delays on incoming messages from all nodes
    for node in nodeSet:
        node.nodeIbStasher.nodelay(ppDelayer)

    # Send one more request
    sendRandomRequest(client1)

    def checkPending3PhaseReqs():
        # Get all replicas that have their primary status decided
        reps = [rep for rep in nodeA.replicas if rep.isPrimary is not None]
        # Atleast one replica should have its primary status decided
        assert len(reps) > 0
        for r in reps:  # type: TestReplica
            logging.debug("primary status for replica {} is {}"
                          .format(r, r.primaryNames))
            assert len(r.threePhaseMsgsForLaterView) > 0

    # NodeA should now have pending 3 phase request for a later view
    looper.run(eventually(checkPending3PhaseReqs, retryWait=1, timeout=30))
