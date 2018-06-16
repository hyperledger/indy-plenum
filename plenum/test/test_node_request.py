import pytest

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from stp_core.loop.looper import Looper
from plenum.common.messages.node_messages import \
    PrePrepare, Prepare, Commit
from plenum.test import waits
from plenum.test.greek import genNodeNames
from plenum.test.helper import assertLength, addNodeBack, \
    getPendingRequestsForReplica, sdk_send_random_and_check
from plenum.test.test_node import TestNode, TestNodeSet, \
    checkPoolReady, genNodeReg

whitelist = ['cannot process incoming PREPARE']
logger = getlogger()


def testReqExecWhenReturnedByMaster(looper, txnPoolNodeSet,
                                    sdk_pool_handle,
                                    sdk_wallet_client):
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              1)

    async def chk():
        for node in txnPoolNodeSet:
            entries = node.spylog.getAll(
                node.processOrdered.__name__)
            for entry in entries:
                arg = entry.params['ordered']
                result = entry.result
                if arg.instId == node.instances.masterId:
                    assert result
                else:
                    assert result is False

    timeout = waits.expectedOrderingTime(
        txnPoolNodeSet[0].instances.count)
    looper.run(eventually(chk, timeout=timeout))


@pytest.mark.skip('Since primary is selected immediately now')
def testPrePrepareWhenPrimaryStatusIsUnknown(tdir_for_func):
    nodeNames = genNodeNames(4)
    nodeReg = genNodeReg(names=nodeNames)
    with TestNodeSet(nodeReg=nodeReg, tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as looper:
            prepareNodeSet(looper, nodeSet)

            nodeA, nodeB, nodeC, nodeD = tuple(
                addNodeBack(
                    nodeSet, looper, nodeNames[i]) for i in range(
                    0, 4))

            # Since primary selection is round robin, A and B will be primaries

            # Nodes C and D delays self nomination so A and B can become
            # primaries
            # nodeC.delaySelfNomination(10)
            # nodeD.delaySelfNomination(10)

            # Node D delays receiving PRIMARY messages from all nodes so it
            # will not know whether it is primary or not

            # delayD = 5
            # nodeD.nodeIbStasher.delay(delayerMsgTuple(delayD, Primary))

            checkPoolReady(looper=looper, nodes=nodeSet)

            # client1, wal = setupClient(looper, nodeSet, tmpdir=tdir_for_func)
            # request = sendRandomRequest(wal, client1)

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

            def assert_msg_count(typ, count):
                assert len(getPendingRequestsForReplica(nodeD.replicas[instNo],
                                                        typ)) == count

            # Node D should have 1 pending PRE-PREPARE request
            timeout = waits.expectedPrePrepareTime(len(nodeSet))
            looper.run(eventually(assert_msg_count, PrePrepare, 1,
                                  retryWait=1, timeout=timeout))

            # Node D should have 2 pending PREPARE requests(from node B and C)
            timeout = waits.expectedPrepareTime(len(nodeSet))
            looper.run(eventually(assert_msg_count, Prepare, 2, retryWait=1,
                                  timeout=timeout))

            # Its been checked above that replica stashes 3 phase messages in
            # lack of primary, now avoid delay (fix the network)
            nodeD.nodeIbStasher.reset_delays_and_process_delayeds()

            # Node D should have no pending PRE-PREPARE, PREPARE or COMMIT
            # requests
            for reqType in [PrePrepare, Prepare, Commit]:
                looper.run(
                    eventually(
                        lambda: assertLength(
                            getPendingRequestsForReplica(
                                nodeD.replicas[instNo],
                                reqType),
                            0),
                        retryWait=1,
                        timeout=delayD))  # wait little more than delay


async def checkIfPropagateRecvdFromNode(recvrNode: TestNode,
                                        senderNode: TestNode, key: str):
    assert key in recvrNode.requests
    assert senderNode.name in recvrNode.requests[key].propagates


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
