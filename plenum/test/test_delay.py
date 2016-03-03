import logging

import pytest
from plenum.test.eventually import eventually

from plenum.common.looper import Looper
from plenum.server.node import Node
from plenum.test.helper import TestNodeSet, checkNodesConnected, prepareNodeSet, \
    randomMsg, sendMsgAndCheck, addNodeBack, ensureElectionsDone, \
    delayerMsgTuple


def assertExp(condition):
    assert condition


def testTestNodeDelay(tdir_for_func):
    nodeNames = {"testA", "testB"}
    with TestNodeSet(names=nodeNames, tmpdir=tdir_for_func) as nodes:
        nodeA = nodes.getNode("testA")
        nodeB = nodes.getNode("testB")

        with Looper(nodes) as looper:
            for n in nodes:
                n.startKeySharing()

            logging.debug("connect")
            looper.run(checkNodesConnected(nodes))
            logging.debug("send one message, without delay")
            msg = randomMsg()
            looper.run(sendMsgAndCheck(nodes, nodeA, nodeB, msg, 1))
            logging.debug("set delay, then send another message and find that "
                          "it doesn't arrive")
            msg = randomMsg()

            nodeB.nodeIbStasher.delay(delayerMsgTuple(5, type(msg), nodeA.name))

            with pytest.raises(AssertionError):
                looper.run(sendMsgAndCheck(nodes, nodeA, nodeB, msg, 3))
            logging.debug("but then find that it arrives after the delay "
                          "duration has passed")
            looper.run(sendMsgAndCheck(nodes, nodeA, nodeB, msg, 2))
            logging.debug(
                    "reset the delay, and find another message comes quickly")
            nodeB.nodeIbStasher.resetDelays()
            msg = randomMsg()
            looper.run(sendMsgAndCheck(nodes, nodeA, nodeB, msg, 1))


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
            looper.run(
                    eventually(lambda: assertExp(nodeA.isReady()), retryWait=1,
                               timeout=5))

            # Elections should be done
            ensureElectionsDone(looper=looper, nodes=nodeSet, retryWait=1,
                                timeout=10)

            # node A should not have any primary replica
            looper.run(
                    eventually(lambda: assertExp(not nodeA.hasPrimary),
                               retryWait=1,
                               timeout=10))

            # Make sure that after at the most 30 seconds, nodeA's
            # `startElection` is called
            looper.run(eventually(lambda: assertExp(
                    len(nodeA.spylog.getAll(
                            Node.decidePrimaries.__name__)) > 0),
                                  retryWait=1, timeout=30))
