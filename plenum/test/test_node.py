import logging

import pytest

from plenum.test.helper import TestNode
from plenum.test.node_helpers.node_helper import getProtocolInstanceNums
from plenum.test.testing_utils import adict

from plenum.common.util import getMaxFailures
from plenum.test.helper import checkNodesConnected, randomMsg, \
    sendMsgAndCheck, checkProtocolInstanceSetup, msgAll


nodeCount = 4


@pytest.fixture(scope="module")
def pool(looper, nodeSet):
    for n in nodeSet:  # type: TestNode
        n.startKeySharing()
    looper.run(checkNodesConnected(nodeSet))
    checkProtocolInstanceSetup(looper, nodeSet, timeout=5)
    return adict(looper=looper, nodeset=nodeSet)


def testConnectNodes(pool):
    pass


def testAllBroadcast(pool):
    pool.looper.run(msgAll(pool.nodeset))


def testMsgSendingTime(pool, nodeReg):
    nodeNames = list(nodeReg.keys())
    msg = randomMsg()
    pool.looper.run(
            sendMsgAndCheck(pool.nodeset,
                            nodeNames[0],
                            nodeNames[1],
                            msg,
                            1))


def testCorrectNumOfProtocolInstances(pool):
    fValue = getMaxFailures(len(pool.nodeSet))
    for node in pool.nodeSet:
        # num of protocol instances running on a node must be f + 1
        assert len(getProtocolInstanceNums(node)) == fValue + 1
        # There should be one running and up master Instance
        assert node.masterId is not None
        # There should be exactly f non master instances
        assert len(node.backupIds) == fValue


def testCorrectNumOfReplicas(pool):
    fValue = getMaxFailures(len(pool.nodeSet))
    for node in pool.nodeSet:
        # num of replicas running on a single node must be f + 1
        assert len(node.replicas) == fValue + 1
        # num of primary nodes is <= 1
        numberOfPrimary = len([r for r in node.replicas if r.isPrimary])
        assert numberOfPrimary <= 1
        for instId in getProtocolInstanceNums(node):
            # num of replicas for a instance on a node must be 1
            assert len([node.replicas[instId]]) == 1 and \
                   node.replicas[instId].instId == instId
            # num of primary on every protocol instance is 1
            numberOfPrimary = len([node for node in pool.nodeSet
                                   if node.replicas[instId].isPrimary])
            assert numberOfPrimary == 1
