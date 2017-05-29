from stp_core.loop.eventually import eventually

from plenum.test import waits
from plenum.test.delayers import ppDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica


TestRunningTimeLimitSec = 300


def testPrimaryRecvs3PhaseMessageOutsideWatermarks(tconf, chkFreqPatched, looper,
                                                   txnPoolNodeSet, client1,
                                                   wallet1, client1Connected):
    """
    One of the primary starts getting lot of requests, more than his log size
    and queues up requests since they will go beyond its watermarks. This
    happens since other nodes are slow in processing its PRE-PREPARE.
    Eventually this primary will send PRE-PREPARE for all requests and those
    requests will complete
    """
    delay = 5
    instId = 1
    reqsToSend = 2*chkFreqPatched.LOG_SIZE + 1
    npr = getNonPrimaryReplicas(txnPoolNodeSet, instId)
    pr = getPrimaryReplica(txnPoolNodeSet, instId)
    from plenum.server.replica import TPCStat
    orderedCount = pr.stats.get(TPCStat.OrderSent)

    for r in npr:
        r.node.nodeIbStasher.delay(ppDelay(delay, instId))

    def chk():
        assert orderedCount + reqsToSend == pr.stats.get(TPCStat.OrderSent)

    print('Sending {} requests'.format(reqsToSend))
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, reqsToSend, 1)
    looper.run(eventually(chk, retryWait=1, timeout=3))
