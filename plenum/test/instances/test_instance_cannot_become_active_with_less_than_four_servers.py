from typing import Iterable

from plenum.common.eventually import eventually
from plenum.common.log import getlogger
from plenum.common.looper import Looper
from plenum.common.startable import Status
from plenum.test.greek import genNodeNames
from plenum.test.helper import addNodeBack, ordinal
from plenum.test.test_node import TestNodeSet, checkNodesConnected, \
    checkNodeRemotes
from plenum.test.test_stack import CONNECTED, JOINED_NOT_ALLOWED

whitelist = ['discarding message']

logger = getlogger()


# noinspection PyIncorrectDocstring
def testProtocolInstanceCannotBecomeActiveWithLessThanFourServers(
        tdir_for_func):
    """
    A protocol instance must have at least 4 nodes to come up.
    The status of the nodes will change from starting to started only after the
    addition of the fourth node to the system.
    """
    nodeCount = 16
    f = 5
    minimumNodesToBeUp = 16 - f

    nodeNames = genNodeNames(nodeCount)
    with TestNodeSet(names=nodeNames, tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as looper:

            # for n in nodeSet:
            #     n.startKeySharing()

            # helpers

            def genExpectedStates(connecteds: Iterable[str]):
                return {
                    nn: CONNECTED if nn in connecteds else JOINED_NOT_ALLOWED
                    for nn in nodeNames}

            def checkNodeStatusRemotesAndF(expectedStatus: Status,
                                           nodeIdx: int):
                for node in nodeSet.nodes.values():
                    checkNodeRemotes(node,
                                     genExpectedStates(nodeNames[:nodeIdx + 1]))
                    assert node.status == expectedStatus

            def addNodeBackAndCheck(nodeIdx: int, expectedStatus: Status):
                logger.info("Add back the {} node and see status of {}".
                             format(ordinal(nodeIdx + 1), expectedStatus))
                addNodeBack(nodeSet, looper, nodeNames[nodeIdx])
                looper.run(
                        eventually(checkNodeStatusRemotesAndF, expectedStatus,
                                   nodeIdx,
                                   retryWait=1, timeout=30))

            # tests

            logger.debug("Sharing keys")
            looper.run(checkNodesConnected(nodeSet))

            logger.debug("Remove all the nodes")
            for n in nodeNames:
                looper.removeProdable(nodeSet.nodes[n])
                nodeSet.removeNode(n, shouldClean=False)

            logger.debug("Add nodes back one at a time")
            for i in range(nodeCount):
                nodes = i + 1
                if nodes < minimumNodesToBeUp:
                    expectedStatus = Status.starting
                elif nodes < nodeCount:
                    expectedStatus = Status.started_hungry
                else:
                    expectedStatus = Status.started
                addNodeBackAndCheck(i, expectedStatus)
