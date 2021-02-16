import pytest

from stp_core.loop.eventually import eventually, slowFactor
from stp_core.common.log import getlogger
from stp_core.loop.looper import Looper
from plenum.server.node import Node
from plenum.test import waits
from plenum.test.delayers import delayerMsgTuple
from plenum.test.helper import sendMessageAndCheckDelivery
from plenum.test.msgs import TestMsg
from plenum.test.test_node import TestNodeSet, checkNodesConnected, \
    ensureElectionsDone, prepareNodeSet

logger = getlogger()

nodeCount = 2


@pytest.mark.skipif('sys.platform == "win32"', reason='SOV-457')
def testTestNodeDelay(looper, txnPoolNodeSet):
    looper.run(checkNodesConnected(txnPoolNodeSet))
    nodeA = txnPoolNodeSet[0]
    nodeB = txnPoolNodeSet[1]
    # send one message, without delay
    looper.run(sendMessageAndCheckDelivery(nodeA, nodeB))

    # set delay, then send another message
    # and find that it doesn't arrive
    delay = 5 * waits.expectedNodeToNodeMessageDeliveryTime()
    nodeB.nodeIbStasher.delay(
        delayerMsgTuple(delay, TestMsg, nodeA.name)
    )
    with pytest.raises(AssertionError):
        looper.run(sendMessageAndCheckDelivery(nodeA, nodeB))

    # but then find that it arrives after the delay
    # duration has passed
    timeout = waits.expectedNodeToNodeMessageDeliveryTime() + delay
    looper.run(sendMessageAndCheckDelivery(nodeA, nodeB,
                                           customTimeout=timeout))

    # reset the delay, and find another message comes quickly
    nodeB.nodeIbStasher.reset_delays_and_process_delayeds()
    looper.run(sendMessageAndCheckDelivery(nodeA, nodeB))
