from plenum.test import waits
from plenum.test.checkpoints.helper import chkChkpoints, checkRequestCounts
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from stp_core.loop.eventually import eventually


def testRequestOlderThanStableCheckpointRemoved(chkFreqPatched, looper,
                                                txnPoolNodeSet, client1,
                                                wallet1, client1Connected,
                                                reqs_for_checkpoint):
    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))

    def send_and_wait_replies(num_reqs):
        return sendReqsToNodesAndVerifySuffReplies(looper,
                                                   wallet1,
                                                   client1,
                                                   num_reqs)

    max_batch_size = chkFreqPatched.Max3PCBatchSize

    # Send some requests (insufficient for checkpoint),
    # wait replies and check that current checkpoint is not stable
    reqs = send_and_wait_replies(reqs_for_checkpoint - max_batch_size)
    total_checkpoints = 1
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, total_checkpoints,
                          retryWait=1, timeout=timeout))
    chk_freq = chkFreqPatched.CHK_FREQ
    checkRequestCounts(txnPoolNodeSet, len(reqs), chk_freq - 1, 1)

    # Send some more requests to cause checkpoint stabilization
    send_and_wait_replies(max_batch_size)

    # Check that checkpoint is stable now
    # and verify that requests for it were removed
    stable_checkpoint_id = 0
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, total_checkpoints,
                          stable_checkpoint_id, retryWait=1, timeout=timeout))
    checkRequestCounts(txnPoolNodeSet, 0, 0, 0)

    # Send some more requests to cause new checkpoint
    send_and_wait_replies(reqs_for_checkpoint + 1)
    total_checkpoints = 2
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, total_checkpoints,
                          stable_checkpoint_id, retryWait=1, timeout=timeout))

    checkRequestCounts(txnPoolNodeSet, 1, 1, 1)
