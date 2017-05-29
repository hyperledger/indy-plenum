from stp_core.loop.eventually import eventually
from plenum.test import waits
from plenum.test.checkpoints.helper import chkChkpoints
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies


TestRunningTimeLimitSec = 300


def checkRequestCounts(nodes, count):
    for node in nodes:
        assert len(node.requests) == count
        for r in node.replicas:
            assert len(r.commits) == count
            assert len(r.prepares) == count


def testRequestOlderThanStableCheckpointRemoved(chkFreqPatched, looper,
                                                txnPoolNodeSet, client1,
                                                wallet1, client1Connected):
    reqs = sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                               chkFreqPatched.CHK_FREQ-1, 1)
    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, retryWait=1,
                          timeout=timeout))
    checkRequestCounts(txnPoolNodeSet, len(reqs))
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1, 1)

    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, 0, retryWait=1,
                          timeout=timeout))
    checkRequestCounts(txnPoolNodeSet, 0)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        3*chkFreqPatched.CHK_FREQ + 1, 1)

    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 2, 0, retryWait=1,
                          timeout=timeout))
    checkRequestCounts(txnPoolNodeSet, 1)
