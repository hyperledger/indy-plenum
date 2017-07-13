import math

import pytest

from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

from plenum.test import waits
from plenum.test.delayers import ppDelay, pDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica


TestRunningTimeLimitSec = 300


logger = getlogger()


@pytest.fixture(scope="module")
def tconf(tconf, request):
    # Delaying perf check as the test sends a lot of requests with delays
    old_freq = tconf.PerfCheckFreq
    tconf.PerfCheckFreq = 30

    def reset():
        tconf.PerfCheckFreq = old_freq

    request.addfinalizer(reset)
    return tconf


def testPrimaryRecvs3PhaseMessageOutsideWatermarks(tconf, chkFreqPatched, looper,
                                                   txnPoolNodeSet, client1,
                                                   wallet1, client1Connected,
                                                   reqs_for_logsize):
    """
    One of the primary starts getting lot of requests, more than his log size
    and queues up requests since they will go beyond its watermarks. This
    happens since other nodes are slow in processing its PRE-PREPARE.
    Eventually this primary will send PRE-PREPARE for all requests and those
    requests will complete
    """
    delay = 3
    instId = 1
    reqs_to_send = 2*reqs_for_logsize + 1
    logger.debug('Will send {} requests'.format(reqs_to_send))

    npr = getNonPrimaryReplicas(txnPoolNodeSet, instId)
    pr = getPrimaryReplica(txnPoolNodeSet, instId)
    from plenum.server.replica import TPCStat
    orderedCount = pr.stats.get(TPCStat.OrderSent)

    for r in npr:
        r.node.nodeIbStasher.delay(ppDelay(delay, instId))
        r.node.nodeIbStasher.delay(pDelay(delay, instId))

    tm_exec_1_batch = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    batch_count = math.ceil(reqs_to_send / tconf.Max3PCBatchSize)
    total_timeout = (tm_exec_1_batch + delay) * batch_count

    def chk():
        assert orderedCount + batch_count == pr.stats.get(TPCStat.OrderSent)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, reqs_to_send)
    looper.run(eventually(chk, retryWait=1, timeout=total_timeout))
