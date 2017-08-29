from plenum.common.config_util import getConfig
from stp_core.loop.eventually import eventually
from stp_core.loop.looper import Looper
from plenum.server.monitor import Monitor
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import TestNodeSet

config = getConfig()


def testPostingThroughput(postingStatsEnabled,
                          decreasedMonitoringTimeouts,
                          looper: Looper,
                          nodeSet: TestNodeSet,
                          wallet1, client1):
    """
    The throughput after `DashboardUpdateFreq` seconds and before sending any
    requests should be zero.
    Send `n` requests in less than `ThroughputWindowSize` seconds and the
    throughput till `ThroughputWindowSize` should consider those `n` requests.
    After `ThroughputWindowSize` seconds the throughput should be zero
    Test `totalRequests` too.
    """

    # We are sleeping for this window size, because we need to clear previous
    # values that were being stored for this much time in tests
    looper.runFor(config.ThroughputWindowSize)

    reqCount = 10
    for node in nodeSet:
        assert node.monitor.highResThroughput == 0
        assert node.monitor.totalRequests == 0

    sendReqsToNodesAndVerifySuffReplies(looper,
                                        wallet1,
                                        client1,
                                        reqCount,
                                        nodeSet.f)

    for node in nodeSet:
        assert len(node.monitor.orderedRequestsInLast) == reqCount
        assert node.monitor.highResThroughput > 0
        assert node.monitor.totalRequests == reqCount
        # TODO: Add implementation to actually call firebase plugin
        # and test if firebase plugin is sending total request count
        # if node is primary

    looper.runFor(config.DashboardUpdateFreq)

    for node in nodeSet:
        node.monitor.spylog.count(Monitor.sendThroughput.__name__) > 0

    # Run for latency window duration so that `orderedRequestsInLast`
    # becomes empty
    looper.runFor(config.ThroughputWindowSize)

    def chk():
        for node in nodeSet:
            assert len(node.monitor.orderedRequestsInLast) == 0
            assert node.monitor.highResThroughput == 0
            assert node.monitor.totalRequests == reqCount

    timeout = config.ThroughputWindowSize
    looper.run(eventually(chk, retryWait=1, timeout=timeout))


def testPostingLatency(postingStatsEnabled,
                       decreasedMonitoringTimeouts,
                       looper: Looper,
                       nodeSet: TestNodeSet,
                       wallet1, client1):
    """
    The latencies (master as well as average of backups) after
    `DashboardUpdateFreq` seconds and before sending any requests should be zero.
    Send `n` requests in less than `LatencyWindowSize` seconds and the
    latency till `LatencyWindowSize` should consider those `n` requests.
    After `LatencyWindowSize` seconds the latencies should be zero
    """
    # Run for latency window duration so that `latenciesByMasterInLast` and
    # `latenciesByBackupsInLast` become empty
    looper.runFor(config.LatencyWindowSize)
    reqCount = 10
    for node in nodeSet:
        assert node.monitor.masterLatency == 0
        assert node.monitor.avgBackupLatency == 0

    sendReqsToNodesAndVerifySuffReplies(looper,
                                        wallet1,
                                        client1,
                                        reqCount,
                                        nodeSet.f)

    for node in nodeSet:
        assert node.monitor.masterLatency > 0
        assert node.monitor.avgBackupLatency > 0

    looper.runFor(config.DashboardUpdateFreq)

    for node in nodeSet:
        node.monitor.spylog.count(Monitor.sendLatencies.__name__) > 0

    # Run for latency window duration so that `latenciesByMasterInLast` and
    # `latenciesByBackupsInLast` become empty
    looper.runFor(config.LatencyWindowSize)

    def chk():
        for node in nodeSet:
            assert node.monitor.masterLatency == 0
            assert node.monitor.avgBackupLatency == 0

    timeout = config.LatencyWindowSize
    looper.run(eventually(chk, retryWait=1, timeout=timeout))
