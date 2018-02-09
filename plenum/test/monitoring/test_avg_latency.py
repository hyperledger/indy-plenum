from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.pool_transactions.conftest import looper


nodeCount = 4
logger = getlogger()


def testAvgReqLatency(looper, tconf, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    """
    Checking if average latency is being set
    """
    _, wallet_did = sdk_wallet_client
    for i in range(5):
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
