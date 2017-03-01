import pytest

from plenum.test.test_node import TestNode, checkProtocolInstanceSetup
from plenum.test.node_helpers.node_helper import getProtocolInstanceNums
from plenum.common.util import getMaxFailures, adict
from plenum.test.helper import checkNodesConnected, sendMsgAndCheck, msgAll
from plenum.test.msgs import randomMsg

nodeCount = 4


# @pytest.fixture(scope="module")
# def setup(request, tdir, nodeReg):
#     for name in nodeReg:
#         pass


@pytest.fixture(scope="module")
def pool(looper, nodeSet):
    # for n in nodeSet:  # type: TestNode
    #     n.startKeySharing()
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
                            timeout=1))


def testCorrectNumOfProtocolInstances(pool):
    fValue = getMaxFailures(len(pool.nodeset))
    for node in pool.nodeset:
        # num of protocol instances running on a node must be f + 1
        assert len(getProtocolInstanceNums(node)) == fValue + 1
        # There should be one running and up master Instance
        assert node.instances.masterId is not None
        # There should be exactly f non master instances
        assert len(node.instances.backupIds) == fValue


def testCorrectNumOfReplicas(pool):
    fValue = getMaxFailures(len(pool.nodeset))
    for node in pool.nodeset:
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
            numberOfPrimary = len([node for node in pool.nodeset
                                   if node.replicas[instId].isPrimary])
            assert numberOfPrimary == 1
