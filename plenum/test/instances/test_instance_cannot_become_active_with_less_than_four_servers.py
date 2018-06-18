from typing import Iterable

import pytest

from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.startable import Status
from plenum.test.greek import genNodeNames
from plenum.test.helper import addNodeBack, ordinal
from plenum.test.node_request.helper import get_node_by_name
from plenum.test.test_node import checkNodesConnected, \
    checkNodeRemotes
from plenum.test.test_stack import CONNECTED, JOINED_NOT_ALLOWED
from plenum.test import waits

logger = getlogger()
whitelist = ['Consensus for ReqId:']

nodeCount = 13
f = 4
minimumNodesToBeUp = nodeCount - f


@pytest.fixture(scope="function", autouse=True)
def limitTestRunningTime():
    return 200

@pytest.fixture(scope="module")
def tconf(tconf):
    old_timeout_restricted = tconf.RETRY_TIMEOUT_RESTRICTED
    old_timeout_not_restricted = tconf.RETRY_TIMEOUT_NOT_RESTRICTED
    tconf.RETRY_TIMEOUT_RESTRICTED = 2
    tconf.RETRY_TIMEOUT_NOT_RESTRICTED = 2
    yield tconf

    tconf.RETRY_TIMEOUT_RESTRICTED = old_timeout_restricted
    tconf.RETRY_TIMEOUT_NOT_RESTRICTED = old_timeout_not_restricted


# noinspection PyIncorrectDocstring
def testProtocolInstanceCannotBecomeActiveWithLessThanFourServers(
        txnPoolNodeSet, looper, tconf, tdir):
    """
    A protocol instance must have at least 4 nodes to come up.
    The status of the nodes will change from starting to started only after the
    addition of the fourth node to the system.
    """

    nodeNames = genNodeNames(nodeCount)
    current_node_set = list(txnPoolNodeSet)

    def genExpectedStates(connecteds: Iterable[str]):
        return {
            nn: CONNECTED if nn in connecteds else JOINED_NOT_ALLOWED
            for nn in nodeNames}

    def checkNodeStatusRemotesAndF(expectedStatus: Status,
                                   nodeIdx: int):
        for node in current_node_set:
            checkNodeRemotes(node,
                             genExpectedStates(nodeNames[:nodeIdx + 1]))
            assert node.status == expectedStatus

    def addNodeBackAndCheck(nodeIdx: int, expectedStatus: Status):
        logger.info("Add back the {} node and see status of {}".
                    format(ordinal(nodeIdx + 1), expectedStatus))
        addNodeBack(
            current_node_set, looper,
            get_node_by_name(txnPoolNodeSet, nodeNames[nodeIdx]),
            tconf, tdir)
        looper.run(checkNodesConnected(current_node_set))
        timeout = waits.expectedNodeStartUpTimeout() + \
                  waits.expectedPoolInterconnectionTime(len(current_node_set))
        # TODO: Probably it's better to modify waits.* functions
        timeout *= 1.5
        looper.run(eventually(checkNodeStatusRemotesAndF,
                              expectedStatus,
                              nodeIdx,
                              retryWait=1, timeout=timeout))

    logger.debug("Sharing keys")
    looper.run(checkNodesConnected(current_node_set))

    logger.debug("Remove all the nodes")
    for n in nodeNames:
        node_n = get_node_by_name(current_node_set, n)
        disconnect_node_and_ensure_disconnected(looper,
                                                current_node_set,
                                                n,
                                                timeout=nodeCount,
                                                stopNode=True)
        current_node_set.remove(node_n)

    # looper.runFor(10)

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
