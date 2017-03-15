from plenum.common.eventually import eventually
from plenum.test.checkpoints.conftest import CHK_FREQ
from plenum.test.checkpoints.helper import chkChkpoints
from plenum.test.delayers import cDelay, pDelay, ppDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    getPrimaryReplica
from plenum.test.test_node import getNonPrimaryReplicas


def checkRequestCounts(nodes, count):
    for node in nodes:
        assert len(node.requests) == count
        for r in node.replicas:
            assert len(r.commits) == count
            assert len(r.prepares) == count
            assert len(r.ordered) == count


def testRequestOlderThanStableCheckpointRemoved(chkFreqPatched, looper,
                                                txnPoolNodeSet, client1,
                                                wallet1, client1Connected):
    reqs = sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                               CHK_FREQ-1, 1)
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, retryWait=1))
    checkRequestCounts(txnPoolNodeSet, len(reqs))
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1, 1)
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, 0, retryWait=1))
    checkRequestCounts(txnPoolNodeSet, 0)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        3*CHK_FREQ + 1, 1)
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 2, 0, retryWait=1))
    checkRequestCounts(txnPoolNodeSet, 1)


def testStableCheckpointWhenOneInstanceSlow(chkFreqPatched, looper,
                                            txnPoolNodeSet, client1,
                                            wallet1, client1Connected):
    pr = getPrimaryReplica(txnPoolNodeSet, 1)
    slowNode = pr.node
    otherNodes = [n for n in txnPoolNodeSet if n != slowNode]
    for n in otherNodes:
        n.nodeIbStasher.delay(ppDelay(5, 1))

    discardCounts = {}
    for n in otherNodes:
        discardCounts[n.name] = len(n.replicas[1].spylog.getAll(
            n.replicas[1].discard.__name__))

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, CHK_FREQ, 1)
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, 0, retryWait=1,
                          timeout=15))
    for n in otherNodes:
        c = len(n.replicas[1].spylog.getAll(n.replicas[0].discard.__name__))
        assert discardCounts[n.name] < c

    looper.runFor(10)
    print(10)

