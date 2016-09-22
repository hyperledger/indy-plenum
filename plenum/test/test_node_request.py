from pprint import pprint

from plenum.common.types import PrePrepare, Prepare, \
    Commit, Primary
from plenum.common.util import getlogger
from plenum.test.eventually import eventually
from plenum.test.greek import genNodeNames
from plenum.test.helper import TestNodeSet, setupNodesAndClient, \
    sendRandomRequest, genNodeReg, \
    prepareNodeSet, ensureElectionsDone, setupClient, checkPoolReady, \
    assertLength, TestNode, \
    addNodeBack, checkSufficientRepliesRecvd, \
    getPendingRequestsForReplica, checkRequestReturnedToNode, delayerMsgTuple

from plenum.common.looper import Looper
from plenum.test.profiler import profile_this

whitelist = ['cannot process incoming PREPARE']
logger = getlogger()


def testReqExecWhenReturnedByMaster(tdir_for_func):
    with TestNodeSet(count=4, tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as looper:
            for n in nodeSet:
                n.startKeySharing()
            client1, wallet1 = setupNodesAndClient(looper,
                                                   nodeSet,
                                                   tmpdir=tdir_for_func)
            req = sendRandomRequest(wallet1, client1)
            looper.run(eventually(checkSufficientRepliesRecvd, client1.inBox,
                                  req.reqId, 1,
                                  retryWait=1, timeout=15))
            async def chk():
                for node in nodeSet:
                    entries = node.spylog.getAll(
                        node.processOrdered.__name__)
                    for entry in entries:
                        arg = entry.params['ordered']
                        result = entry.result
                        if arg.instId == node.instances.masterId:
                            assert result
                        else:
                            assert result is None

            looper.run(eventually(chk, timeout=3))


# noinspection PyIncorrectDocstring
def testRequestReturnToNodeWhenPrePrepareNotReceivedByOneNode(tdir_for_func):
    """Test no T-3"""
    nodeNames = genNodeNames(7)
    nodeReg = genNodeReg(names=nodeNames)
    with TestNodeSet(nodeReg=nodeReg, tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as looper:
            prepareNodeSet(looper, nodeSet)
            logger.debug("Add the seven nodes back in")
            # Every node except A delays self nomination so A can become primary
            nodeA = addNodeBack(nodeSet, looper, nodeNames[0])
            for i in range(1, 7):
                node = addNodeBack(nodeSet, looper, nodeNames[i])
                node.delaySelfNomination(15)

            nodeB = nodeSet.getNode(nodeNames[1])
            # Node B delays PREPREPARE from node A(which would be the primary)
            # for a long time.
            nodeB.nodeIbStasher.delay(
                delayerMsgTuple(120, PrePrepare, nodeA.name))

            # Ensure elections are done
            ensureElectionsDone(looper=looper, nodes=nodeSet, retryWait=1,
                                timeout=30)
            assert nodeA.hasPrimary

            instNo = nodeA.primaryReplicaNo
            client1, wallet1 = setupClient(looper, nodeSet, tmpdir=tdir_for_func)
            req = sendRandomRequest(wallet1, client1)

            # All nodes including B should return their ordered requests
            for node in nodeSet:
                looper.run(eventually(checkRequestReturnedToNode, node,
                                      wallet1.defaultId, req.reqId,
                                      req.digest,
                                      instNo, retryWait=1, timeout=30))

            # Node B should not have received the PRE-PREPARE request yet
            replica = nodeB.replicas[instNo]  # type: Replica
            assert len(replica.prePrepares) == 0


def testPrePrepareWhenPrimaryStatusIsUnknown(tdir_for_func):
    nodeNames = genNodeNames(4)
    nodeReg = genNodeReg(names=nodeNames)
    with TestNodeSet(nodeReg=nodeReg, tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as looper:
            prepareNodeSet(looper, nodeSet)

            nodeA, nodeB, nodeC, nodeD = tuple(
                addNodeBack(nodeSet, looper, nodeNames[i]) for i in range(0, 4))

            # Nodes C and D delays self nomination so A and B can become
            # primaries
            nodeC.delaySelfNomination(30)
            nodeD.delaySelfNomination(30)

            # Node D delays receiving PRIMARY messages from all nodes so it
            # will not know whether it is primary or not

            # nodeD.nodestack.delay(delayer(20, PRIMARY))

            nodeD.nodeIbStasher.delay(delayerMsgTuple(20, Primary))

            checkPoolReady(looper=looper, nodes=nodeSet)

            client1, wal = setupClient(looper, nodeSet, tmpdir=tdir_for_func)
            request = sendRandomRequest(wal, client1)

            # TODO Rethink this
            instNo = 0

            for i in range(3):
                node = nodeSet.getNode(nodeNames[i])
                # Nodes A, B and C should have received PROPAGATE request
                # from Node D
                looper.run(
                    eventually(checkIfPropagateRecvdFromNode, node, nodeD,
                               request.identifier,
                               request.reqId, retryWait=1, timeout=10))

            # Node D should have 1 pending PRE-PREPARE request
            def assertOnePrePrepare():
                assert len(getPendingRequestsForReplica(nodeD.replicas[instNo],
                                                        PrePrepare)) == 1

            looper.run(eventually(assertOnePrePrepare, retryWait=1, timeout=10))

            # Node D should have 2 pending PREPARE requests(from node B and C)

            def assertTwoPrepare():
                assert len(getPendingRequestsForReplica(nodeD.replicas[instNo],
                                                        Prepare)) == 2

            looper.run(eventually(assertTwoPrepare, retryWait=1, timeout=10))

            # Node D should have no pending PRE-PREPARE, PREPARE or COMMIT
            # requests
            for reqType in [PrePrepare, Prepare, Commit]:
                looper.run(eventually(lambda: assertLength(
                    getPendingRequestsForReplica(nodeD.replicas[instNo],
                                                 reqType),
                    0), retryWait=1, timeout=20))


async def checkIfPropagateRecvdFromNode(recvrNode: TestNode,
                                        senderNode: TestNode, identifier: str,
                                        reqId: int):
    key = identifier, reqId
    assert key in recvrNode.requests
    assert senderNode.name in recvrNode.requests[key].propagates


# noinspection PyIncorrectDocstring
def testMultipleRequests(tdir_for_func):
    """
    Send multiple requests to the client
    """
    with TestNodeSet(count=7, tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as looper:
            for n in nodeSet:
                n.startKeySharing()

            ss0 = snapshotStats(*nodeSet)
            client, wal = setupNodesAndClient(looper,
                                              nodeSet,
                                              tmpdir=tdir_for_func)
            ss1 = snapshotStats(*nodeSet)

            def x():
                requests = [sendRandomRequest(wal, client) for _ in range(10)]
                for request in requests:
                    looper.run(eventually(
                        checkSufficientRepliesRecvd, client.inBox,
                        request.reqId, 3,
                        retryWait=1, timeout=3 * len(nodeSet)))
                ss2 = snapshotStats(*nodeSet)
                diff = statsDiff(ss2, ss1)

                pprint(ss2)
                print("----------------------------------------------")
                pprint(diff)

            profile_this(x)


def snapshotStats(*nodes):
    return {n.name: n.nodestack.stats.copy() for n in nodes}


def statsDiff(a, b):
    diff = {}
    nodes = set(a.keys()).union(b.keys())
    for n in nodes:
        nas = a.get(n, {})
        nbs = b.get(n, {})

        keys = set(nas.keys()).union(nbs.keys())
        diff[n] = {}
        for k in keys:
            diff[n][k] = nas.get(k, 0) - nbs.get(k, 0)
    return diff
