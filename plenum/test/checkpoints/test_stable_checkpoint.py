from plenum.test import waits
from plenum.test.checkpoints.helper import chkChkpoints, checkRequestCounts
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from stp_core.loop.eventually import eventually


def testRequestOlderThanStableCheckpointRemoved(chkFreqPatched, looper,
                                                txnPoolNodeSet, client1,
                                                wallet1, client1Connected,
                                                reqs_for_checkpoint):
    max_batch_size = chkFreqPatched.Max3PCBatchSize
    chk_freq = chkFreqPatched.CHK_FREQ
    reqs = sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                               reqs_for_checkpoint - max_batch_size,
                                               1)
    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, retryWait=1,
                          timeout=timeout))
    checkRequestCounts(txnPoolNodeSet, len(reqs), chk_freq - 1, 1)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        max_batch_size, 1)

    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, 0, retryWait=1,
                          timeout=timeout))
    checkRequestCounts(txnPoolNodeSet, 0, 0, 0)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        reqs_for_checkpoint + 1, 1)

    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 2, 0, retryWait=1,
                          timeout=timeout))
    checkRequestCounts(txnPoolNodeSet, 1, 1, 1)
