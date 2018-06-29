import types

import pytest

from plenum.common.exceptions import PoolLedgerTimeoutException
from plenum.test.helper import sdk_send_random_and_check
from stp_core.common.log import getlogger

logger = getlogger()

TXN_COUNT = 100


@pytest.fixture(scope="module")
def tconf(tconf):
    old_max = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 10
    yield tconf
    tconf.Max3PCBatchSize = old_max


@pytest.fixture(scope="function", autouse=True)
def limitTestRunningTime():
    return 600

@pytest.mark.skip(reason="Too much request. Needs for checking future implementation")
def test_send_too_much_reqs(looper,
                            txnPoolNodeSet,
                            sdk_pool_handle,
                            sdk_wallet_steward):
    for _ in range(TXN_COUNT):
        sdk_send_random_and_check(looper,
                                  txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_steward,
                                  1)

# @pytest.mark.skip(reason="Too much request. Needs for checking future implementation")
def test_send_with_clientstack_restarts(looper,
                                        txnPoolNodeSet,
                                        sdk_pool_handle,
                                        sdk_wallet_steward):
    orig_method = txnPoolNodeSet[0].checkPerformance

    def check_perf(self):
        self.restart_clientstack()
        logger.info("Restart clientstack on node: {}".format(self))
        orig_method()

    for node in txnPoolNodeSet:
        node.checkPerformance = types.MethodType(check_perf,
                                                 node)
    success_txns = 0
    failed_txns = 0
    for _ in range(TXN_COUNT):
        try:
            sdk_send_random_and_check(looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_steward,
                                      1)
        except PoolLedgerTimeoutException:
            failed_txns += 1
        else:
            success_txns += 1

    logger.info("Count of successful requests: {} "
                "Count of failed requests: {} ".format(success_txns, failed_txns))