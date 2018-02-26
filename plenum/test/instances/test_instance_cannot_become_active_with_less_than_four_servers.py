from typing import Iterable

import pytest
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from stp_core.loop.looper import Looper
from plenum.common.startable import Status
from plenum.test.greek import genNodeNames
from plenum.test.helper import addNodeBack, ordinal
from plenum.test.test_node import TestNodeSet, checkNodesConnected, \
    checkNodeRemotes
from plenum.test.test_stack import CONNECTED, JOINED_NOT_ALLOWED
from plenum.test import waits


whitelist = ['discarding message']

logger = getlogger()


@pytest.fixture(scope="function", autouse=True)
def limitTestRunningTime():
    return 200


# noinspection PyIncorrectDocstring
def testProtocolInstanceCannotBecomeActiveWithLessThanFourServers(
        tconf_for_func, tdir_for_func):
    """
    A protocol instance must have at least 4 nodes to come up.
    The status of the nodes will change from starting to started only after the
    addition of the fourth node to the system.
    """
    nodeCount = 13
    f = 4
    minimumNodesToBeUp = nodeCount - f

    nodeNames = genNodeNames(nodeCount)
    with TestNodeSet(tconf_for_func, names=nodeNames, tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as looper:

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

                timeout = waits.expectedNodeStartUpTimeout() + \
                    waits.expectedPoolInterconnectionTime(len(nodeSet))
                looper.run(eventually(checkNodeStatusRemotesAndF,
                                      expectedStatus,
                                      nodeIdx,
                                      retryWait=1, timeout=timeout))

            logger.debug("Sharing keys")
            looper.run(checkNodesConnected(nodeSet))

            logger.debug("Remove all the nodes")
            for n in nodeNames:
                looper.removeProdable(nodeSet.nodes[n])
                nodeSet.removeNode(n)

            looper.runFor(10)

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
