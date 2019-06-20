import pytest

from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_and_check

nodeCount = 4
logger = getlogger()
txnCount = 5


@pytest.fixture(scope='module')
def tconf(tconf):
    old_min_cnt = tconf.MIN_LATENCY_COUNT
    tconf.MIN_LATENCY_COUNT = txnCount
    yield tconf
    tconf.MIN_LATENCY_COUNT = old_min_cnt

@pytest.mark.skip(reason="Not used now")
def testAvgReqLatency(looper, tconf, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    """
    Checking if average latency is being set
    """
    _, wallet_did = sdk_wallet_client
    for i in range(txnCount):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    for node in txnPoolNodeSet:  # type: Node
        mLat = node.monitor.getAvgLatencyForClient(wallet_did,
                                                   node.instances.masterId)
        bLat = node.monitor.getAvgLatencyForClient(wallet_did,
                                                   *node.instances.backupIds)
        logger.debug("Avg. master latency : {}. Avg. backup latency: {}".
                     format(mLat, bLat))
        assert mLat > 0
        assert bLat > 0
