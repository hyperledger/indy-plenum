import pytest
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from stp_core.loop.eventually import eventually
from plenum.server.monitor import Monitor
from plenum.test.helper import sdk_send_random_and_check


@pytest.fixture(scope='module')
def tconf(tconf):
    old_val = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1
    yield tconf
    tconf.Max3PCBatchSize = old_val


def testPostingThroughput(postingStatsEnabled,
                          decreasedMonitoringTimeouts,
                          looper,
                          txnPoolNodeSet,
                          sdk_wallet_client, sdk_pool_handle):
    """
    The throughput after `DashboardUpdateFreq` seconds and before sending any
    requests should be zero.
    Send `n` requests in less than `ThroughputWindowSize` seconds and the
    throughput till `ThroughputWindowSize` should consider those `n` requests.
    After `ThroughputWindowSize` seconds the throughput should be zero
    Test `totalRequests` too.
    """

    config = decreasedMonitoringTimeouts

    # We are sleeping for this window size, because we need to clear previous
    # values that were being stored for this much time in tests
    looper.runFor(config.ThroughputWindowSize)

    reqCount = 10
    for node in txnPoolNodeSet:
        assert node.monitor.highResThroughput == 0
        assert node.monitor.totalRequests == 0

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              reqCount)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    for node in txnPoolNodeSet:
        assert len(node.monitor.orderedRequestsInLast) == reqCount
        assert node.monitor.highResThroughput > 0
        assert node.monitor.totalRequests == reqCount
        # TODO: Add implementation to actually call firebase plugin
        # and test if firebase plugin is sending total request count
        # if node is primary

    looper.runFor(config.DashboardUpdateFreq)

    for node in txnPoolNodeSet:
        node.monitor.spylog.count(Monitor.sendThroughput.__name__) > 0

    # Run for latency window duration so that `orderedRequestsInLast`
    # becomes empty
    looper.runFor(config.ThroughputWindowSize)

    def chk():
        for node in txnPoolNodeSet:
            assert len(node.monitor.orderedRequestsInLast) == 0
            assert node.monitor.highResThroughput == 0
            assert node.monitor.totalRequests == reqCount

    timeout = config.ThroughputWindowSize
    looper.run(eventually(chk, retryWait=1, timeout=timeout))


def testPostingLatency(postingStatsEnabled,
                       decreasedMonitoringTimeouts,
                       looper,
                       txnPoolNodeSet,
                       sdk_wallet_client, sdk_pool_handle):
    """
    The latencies (master as well as average of backups) after
    `DashboardUpdateFreq` seconds and before sending any requests should be zero.
    Send `n` requests in less than `LatencyWindowSize` seconds and the
    latency till `LatencyWindowSize` should consider those `n` requests.
    After `LatencyWindowSize` seconds the latencies should be zero
    """

    config = decreasedMonitoringTimeouts

    # Run for latency window duration so that `latenciesByMasterInLast` and
    # `latenciesByBackupsInLast` become empty
    looper.runFor(config.LatencyWindowSize)
    reqCount = 10
    for node in txnPoolNodeSet:
        assert node.monitor.masterLatency == 0
        assert node.monitor.avgBackupLatency == 0

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              reqCount)

    for node in txnPoolNodeSet:
        assert node.monitor.masterLatency > 0
        assert node.monitor.avgBackupLatency > 0

    looper.runFor(config.DashboardUpdateFreq)

    for node in txnPoolNodeSet:
        node.monitor.spylog.count(Monitor.sendLatencies.__name__) > 0

    # Run for latency window duration so that `latenciesByMasterInLast` and
    # `latenciesByBackupsInLast` become empty
    looper.runFor(config.LatencyWindowSize)

    def chk():
        for node in txnPoolNodeSet:
            assert node.monitor.masterLatency == 0
            assert node.monitor.avgBackupLatency == 0

    timeout = config.LatencyWindowSize
    looper.run(eventually(chk, retryWait=1, timeout=timeout))
