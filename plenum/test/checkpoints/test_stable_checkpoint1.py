from stp_core.loop.eventually import eventually

from plenum.test import waits
from plenum.test.checkpoints.helper import chkChkpoints
from plenum.test.delayers import ppDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import getPrimaryReplica


def testStableCheckpointWhenOneInstanceSlow(chkFreqPatched, looper,
                                            txnPoolNodeSet, client1,
                                            wallet1, client1Connected,
                                            reqs_for_checkpoint):
    delay = 5
    pr = getPrimaryReplica(txnPoolNodeSet, 1)
    slowNode = pr.node
    otherNodes = [n for n in txnPoolNodeSet if n != slowNode]
    for n in otherNodes:
        n.nodeIbStasher.delay(ppDelay(delay, 1))

    sendReqsToNodesAndVerifySuffReplies(
        looper, wallet1, client1, reqs_for_checkpoint)
    timeout = waits.expectedTransactionExecutionTime(
        len(txnPoolNodeSet)) + delay
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, 0, retryWait=1,
                          timeout=timeout))
