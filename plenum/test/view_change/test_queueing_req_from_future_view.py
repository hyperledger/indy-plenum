from functools import partial

import pytest

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.util import getMaxFailures
from plenum.test import waits
from plenum.test.delayers import ppDelay, icDelay
from plenum.test.helper import sendRandomRequest, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import TestReplica, getNonPrimaryReplicas, \
    checkViewChangeInitiatedForNode

nodeCount = 7

logger = getlogger()


# noinspection PyIncorrectDocstring
def testQueueingReqFromFutureView(delayedPerf, looper, nodeSet, up,
                                  wallet1, client1):
    """
    Test if every node queues 3 Phase requests(PRE-PREPARE, PREPARE and COMMIT)
    that come from a view which is greater than the current view
    """

    f = getMaxFailures(nodeCount)

    # Delay processing of instance change on a node
    delayIcA = 60
    nodeA = nodeSet.Alpha
    nodeA.nodeIbStasher.delay(icDelay(delayIcA))

    nonPrimReps = getNonPrimaryReplicas(nodeSet, 0)
    # Delay processing of PRE-PREPARE from all non primary replicas of master
    # so master's throughput falls and view changes
    delay = 5
    ppDelayer = ppDelay(delay, 0)
    for r in nonPrimReps:
        r.node.nodeIbStasher.delay(ppDelayer)

    timeout = waits.expectedTransactionExecutionTime(len(nodeSet)) + delay
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4,
                                        customTimeoutPerReq=timeout)

    # Every node except Node A should have a view change
    timeout = waits.expectedPoolViewChangeStartedTimeout(len(nodeSet))
    for node in nodeSet:
        if node.name == nodeA.name:
            # Node A's view should not have changed yet
            with pytest.raises(AssertionError):
                looper.run(eventually(partial(
                    checkViewChangeInitiatedForNode, node, 1),
                    retryWait=1,
                    timeout=timeout))
        else:
            looper.run(eventually(
                partial(checkViewChangeInitiatedForNode, node, 1),
                retryWait=1,
                timeout=timeout))

    # NodeA should not have any pending 3 phase request for a later view
    for r in nodeA.replicas:  # type: TestReplica
        assert len(r.threePhaseMsgsForLaterView) == 0

    # Reset delays on incoming messages from all nodes
    for node in nodeSet:
        node.nodeIbStasher.nodelay(ppDelayer)

    # Send one more request
    sendRandomRequest(wallet1, client1)

    def checkPending3PhaseReqs():
        # Get all replicas that have their primary status decided
        reps = [rep for rep in nodeA.replicas if rep.isPrimary is not None]
        # At least one replica should have its primary status decided
        assert len(reps) > 0
        for r in reps:  # type: TestReplica
            logger.debug("primary status for replica {} is {}"
                          .format(r, r.primaryNames))
            assert len(r.threePhaseMsgsForLaterView) > 0

    # NodeA should now have pending 3 phase request for a later view
    timeout = waits.expectedPoolViewChangeStartedTimeout(len(nodeSet)) + delayIcA
    looper.run(eventually(checkPending3PhaseReqs, retryWait=1, timeout=timeout))
