from functools import partial

from plenum.test.eventually import eventually

from plenum.common.looper import Looper
from plenum.test.helper import TestNodeSet, getNonPrimaryReplicas, ppDelay, \
    checkProtocolInstanceSetup, checkViewChangeInitiatedForNode, \
    sendReqsToNodesAndVerifySuffReplies

nodeCount = 7


# noinspection PyIncorrectDocstring
def testElectionsAfterViewChange(delayedPerf, looper: Looper,
                                 nodeSet: TestNodeSet, up, wallet1, client1):
    """
    Test that a primary election does happen after a view change
    """

    # Delay processing of PRE-PREPARE from all non primary replicas of master
    # so master's throughput falls
    # and view changes
    nonPrimReps = getNonPrimaryReplicas(nodeSet, 0)
    for r in nonPrimReps:
        r.node.nodeIbStasher.delay(ppDelay(10, 0))

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    # Ensure view change happened for both node and its primary elector
    for node in nodeSet:
        looper.run(eventually(partial(checkViewChangeInitiatedForNode, node, 0),
                              retryWait=1, timeout=20))

    # Ensure elections are done again and pool is setup again with appropriate
    # protocol instances and each protocol instance is setup properly too
    checkProtocolInstanceSetup(looper, nodeSet, retryWait=1, timeout=30)
