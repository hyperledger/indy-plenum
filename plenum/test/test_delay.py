import pytest

from stp_core.loop.eventually import eventually, slowFactor
from stp_core.common.log import getlogger
from stp_core.loop.looper import Looper
from plenum.server.node import Node
from plenum.test import waits
from plenum.test.delayers import delayerMsgTuple
from plenum.test.helper import sendMessageAndCheckDelivery, addNodeBack, assertExp
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


@pytest.mark.skip('Nodes use round robin primary selection')
def testSelfNominationDelay(tdir_for_func):
    nodeNames = ["testA", "testB", "testC", "testD"]
    with TestNodeSet(names=nodeNames, tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as looper:
            prepareNodeSet(looper, nodeSet)

            delay = 30
            # Add node A
            nodeA = addNodeBack(nodeSet, looper, nodeNames[0])
            nodeA.delaySelfNomination(delay)

            nodesBCD = []
            for name in nodeNames[1:]:
                # nodesBCD.append(nodeSet.addNode(name, i+1, AutoMode.never))
                nodesBCD.append(addNodeBack(nodeSet, looper, name))

            # Ensuring that NodeA is started before any other node to demonstrate
            # that it is delaying self nomination
            timeout = waits.expectedNodeStartUpTimeout()
            looper.run(
                eventually(lambda: assertExp(nodeA.isReady()), retryWait=1,
                           timeout=timeout))

            ensureElectionsDone(looper=looper,
                                nodes=nodeSet,
                                retryWait=1)

            # node A should not have any primary replica
            timeout = waits.expectedNodeStartUpTimeout()
            looper.run(
                eventually(lambda: assertExp(not nodeA.hasPrimary),
                           retryWait=1,
                           timeout=timeout))

            # Make sure that after at the most 30 seconds, nodeA's
            # `startElection` is called
            looper.run(eventually(lambda: assertExp(
                len(nodeA.spylog.getAll(
                    Node.decidePrimaries.__name__)) > 0),
                                  retryWait=1, timeout=delay))
