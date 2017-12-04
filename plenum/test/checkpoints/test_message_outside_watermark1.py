import math

from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

from plenum.test import waits
from plenum.test.delayers import ppDelay, pDelay, icDelay
from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica
from plenum.test.view_change.conftest import perf_chk_patched
from plenum.test.sdk.conftest import sdk_pool_name, sdk_pool_handle, sdk_wallet_name,\
    sdk_wallet_handle, sdk_wallet_client, sdk_client_seed
from plenum.test.sdk.helper import send_random_and_check


TestRunningTimeLimitSec = 300
PerfCheckFreq = 30

logger = getlogger()

whitelist = ['received an incorrect digest']


def test_primary_recvs_3phase_message_outside_watermarks(perf_chk_patched, chkFreqPatched, looper, txnPoolNodeSet,
                                                         sdk_pool_handle, sdk_wallet_client, reqs_for_logsize):
    """
    One of the primary starts getting lot of requests, more than his log size
    and queues up requests since they will go beyond its watermarks. This
    happens since other nodes are slow in processing its PRE-PREPARE.
    Eventually this primary will send PRE-PREPARE for all requests and those
    requests will complete
    """
    tconf = perf_chk_patched
    delay = 5
    instId = 0
    reqs_to_send = 2 * reqs_for_logsize + 1
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

    send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, reqs_to_send)
    looper.run(eventually(chk, retryWait=1, timeout=total_timeout))
