from stp_core.loop.eventually import eventually
from plenum.test.checkpoints.conftest import CHK_FREQ
from plenum.test.checkpoints.helper import chkChkpoints
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies


def testCheckpointCreated(chkFreqPatched, looper, txnPoolNodeSet, client1,
                          wallet1, client1Connected):
    """
    After requests less than `CHK_FREQ`, there should be one checkpoint
    on each replica. After `CHK_FREQ`, one checkpoint should become stable
    """
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, CHK_FREQ-1, 1)
    # Deliberately waiting so as to verify that not more than 1 checkpoint is
    # created
    looper.runFor(2)
    chkChkpoints(txnPoolNodeSet, 1)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1, 1)

    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, 0, retryWait=1))


def testOldCheckpointDeleted(chkFreqPatched, looper, txnPoolNodeSet, client1,
                             wallet1, client1Connected):
    """
    Send requests more than twice of `CHK_FREQ`, there should be one new stable
    checkpoint on each replica. The old stable checkpoint should be removed
    """
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 2*CHK_FREQ,
                                        1)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1, 1)

    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 2, 0, retryWait=1))
