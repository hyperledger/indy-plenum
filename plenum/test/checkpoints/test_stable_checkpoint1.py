from stp_core.loop.eventually import eventually
from plenum.test.checkpoints.conftest import CHK_FREQ
from plenum.test.checkpoints.helper import chkChkpoints
from plenum.test.delayers import ppDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import getPrimaryReplica


def testStableCheckpointWhenOneInstanceSlow(chkFreqPatched, looper,
                                            txnPoolNodeSet, client1,
                                            wallet1, client1Connected):
    pr = getPrimaryReplica(txnPoolNodeSet, 1)
    slowNode = pr.node
    otherNodes = [n for n in txnPoolNodeSet if n != slowNode]
    for n in otherNodes:
        n.nodeIbStasher.delay(ppDelay(5, 1))

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, CHK_FREQ, 1)
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, 0, retryWait=1,
                          timeout=15))
