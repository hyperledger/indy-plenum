from typing import Iterable

import pytest

from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_and_check

nodeCount = 4
logger = getlogger()


# noinspection PyIncorrectDocstring
@pytest.mark.skip(reason="Duplicated in testThroughputThreshold")
def testThroughput(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    """
    Checking if the throughput is being set
    """
    for i in range(5):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    for node in txnPoolNodeSet:
        masterThroughput, avgBackupThroughput = node.monitor.getThroughputs(
            node.instances.masterId)
        logger.debug("Master throughput: {}. Avg. backup throughput: {}".
                     format(masterThroughput, avgBackupThroughput))
        assert masterThroughput > 0
        assert avgBackupThroughput > 0
