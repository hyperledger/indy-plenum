from plenum.common.eventually import eventually
from plenum.common.types import Prepare
from plenum.test.checkpoints.conftest import CHK_FREQ
from plenum.test.checkpoints.helper import chkChkpoints
from plenum.test.delayers import cDelay, pDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
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


def test3PhaseMsgsOlderThanStableCheckpointDiscarded(chkFreqPatched, looper,
                                                     txnPoolNodeSet, client1,
                                                     wallet1, client1Connected):
    # TODO: Complete this
    npr = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    slowNode = npr[0].node
    otherNodes = [n for n in txnPoolNodeSet if n != slowNode]
    rep1 = slowNode.replicas[0]

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, CHK_FREQ, 1)
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, 0, retryWait=1,
                          timeout=10))
    oldPrep = Prepare(rep1.instId, rep1.viewNo, 1, '', 112)
    rep1.send(oldPrep)

    looper.runFor(3)

    for n in otherNodes:
        c = len(n.replicas[0].spylog.getAll(n.replicas[0].discard.__name__))
        print(c)


    print(10)

