from pprint import pprint

import pytest
from plenum import config

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from stp_core.loop.looper import Looper
from plenum.common.types import PrePrepare, Prepare, \
    Commit, Primary
from plenum.common.util import getMaxFailures
from plenum.test import waits
from plenum.test.delayers import delayerMsgTuple
from plenum.test.greek import genNodeNames
from plenum.test.helper import setupNodesAndClient, \
    sendRandomRequest, setupClient, \
    assertLength, addNodeBack, waitForSufficientRepliesForRequests, \
    getPendingRequestsForReplica, checkRequestReturnedToNode
from plenum.test.profiler import profile_this
from plenum.test.test_node import TestNode, TestNodeSet, checkPoolReady, \
    ensureElectionsDone, genNodeReg, prepareNodeSet

whitelist = ['cannot process incoming PREPARE']
logger = getlogger()


def testReqExecWhenReturnedByMaster(tdir_for_func):
    with TestNodeSet(count=4, tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as looper:
            client1, wallet1 = setupNodesAndClient(looper,
                                                   nodeSet,
                                                   tmpdir=tdir_for_func)
            req = sendRandomRequest(wallet1, client1)
            waitForSufficientRepliesForRequests(looper, client1,
                                                requests=[req], fVal=1)

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
            timeout = waits.expectedOrderingTime(nodeSet.nodes['Alpha'].instances.count)
            looper.run(eventually(chk, timeout=timeout))


# noinspection PyIncorrectDocstring
@pytest.mark.skip(reason="SOV-539. Implementation changed")
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
            ensureElectionsDone(looper=looper, nodes=nodeSet)
            assert nodeA.hasPrimary

            instNo = nodeA.primaryReplicaNo
            client1, wallet1 = setupClient(looper, nodeSet, tmpdir=tdir_for_func)
            req = sendRandomRequest(wallet1, client1)

            # All nodes including B should return their ordered requests
            for node in nodeSet:
                # TODO set timeout from 'waits' after the test enabled
                looper.run(eventually(checkRequestReturnedToNode, node,
                                      wallet1.defaultId, req.reqId,
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
            delayD = 20
            nodeD.nodeIbStasher.delay(delayerMsgTuple(delayD, Primary))

            checkPoolReady(looper=looper, nodes=nodeSet)

            client1, wal = setupClient(looper, nodeSet, tmpdir=tdir_for_func)
            request = sendRandomRequest(wal, client1)

            # TODO Rethink this
            instNo = 0

            timeout = waits.expectedClientRequestPropagationTime(len(nodeSet))
            for i in range(3):
                node = nodeSet.getNode(nodeNames[i])
                # Nodes A, B and C should have received PROPAGATE request
                # from Node D
                looper.run(
                    eventually(checkIfPropagateRecvdFromNode, node, nodeD,
                               request.identifier,
                               request.reqId, retryWait=1, timeout=timeout))

            # Node D should have 1 pending PRE-PREPARE request
            def assertOnePrePrepare():
                assert len(getPendingRequestsForReplica(nodeD.replicas[instNo],
                                                        PrePrepare)) == 1

            timeout = waits.expectedPrePrepareTime(len(nodeSet))
            looper.run(eventually(assertOnePrePrepare, retryWait=1, timeout=timeout))

            # Node D should have 2 pending PREPARE requests(from node B and C)

            def assertTwoPrepare():
                assert len(getPendingRequestsForReplica(nodeD.replicas[instNo],
                                                        Prepare)) == 2

            timeout = waits.expectedPrePrepareTime(len(nodeSet))
            looper.run(eventually(assertTwoPrepare, retryWait=1, timeout=timeout))

            # Node D should have no pending PRE-PREPARE, PREPARE or COMMIT
            # requests
            for reqType in [PrePrepare, Prepare, Commit]:
                looper.run(eventually(lambda: assertLength(
                    getPendingRequestsForReplica(nodeD.replicas[instNo],
                                                 reqType),
                    0), retryWait=1, timeout=delayD))


async def checkIfPropagateRecvdFromNode(recvrNode: TestNode,
                                        senderNode: TestNode, identifier: str,
                                        reqId: int):
    key = identifier, reqId
    assert key in recvrNode.requests
    assert senderNode.name in recvrNode.requests[key].propagates


# noinspection PyIncorrectDocstring
@pytest.mark.skip(reason="INDY-76. ZStack does not have any mechanism to have "
                         "stats either remove this once raet is removed "
                         "or implement a `stats` feature in ZStack")
def testMultipleRequests(tdir_for_func):
    """
    Send multiple requests to the client
    """
    with TestNodeSet(count=7, tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as looper:
            # for n in nodeSet:
            #     n.startKeySharing()

            # TODO: ZStack does not have any mechanism to have stats,
            # either remove this once raet is removed or implement a `stats`
            # feature in ZStack
            if not config.UseZStack:
                ss0 = snapshotStats(*nodeSet)
            client, wal = setupNodesAndClient(looper,
                                              nodeSet,
                                              tmpdir=tdir_for_func)
            if not config.UseZStack:
                ss1 = snapshotStats(*nodeSet)

            def x():
                requests = [sendRandomRequest(wal, client) for _ in range(10)]
                waitForSufficientRepliesForRequests(looper, client,
                                                    requests=requests, fVal=3)

                ss2 = snapshotStats(*nodeSet)
                diff = statsDiff(ss2, ss1)

                if not config.UseZStack:
                    ss2 = snapshotStats(*nodeSet)
                    diff = statsDiff(ss2, ss1)

                    pprint(ss2)
                    print("----------------------------------------------")
                    pprint(diff)

            profile_this(x)


def testClientSendingSameRequestAgainBeforeFirstIsProcessed(looper, nodeSet,
                                                            up, wallet1,
                                                            client1):
    size = len(client1.inBox)
    req = sendRandomRequest(wallet1, client1)
    client1.submitReqs(req)
    waitForSufficientRepliesForRequests(looper, client1, requests=[req])
    # Only REQACK will be sent twice by the node but not REPLY
    assert len(client1.inBox) == size + 12


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
