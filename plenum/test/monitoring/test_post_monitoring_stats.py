from plenum.common.looper import Looper
from plenum.server.monitor import Monitor
from plenum.test.eventually import eventually
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    TestNodeSet
from plenum.common.util import getConfig


config = getConfig()


def testPostingThroughput(postingStatsEnabled, looper: Looper, nodeSet: TestNodeSet,
                          client1):
    """
    The throughput after `DashboardUpdateFreq` seconds and before sending any
    requests should be zero.
    Send `n` requests in less than `ThroughputWindowSize` seconds and the
    throughput till `ThroughputWindowSize` should consider those `n` requests.
    After `ThroughputWindowSize` seconds the throughput should be zero
    Test `totalRequests` too.
    """
    reqCount = 10
    for node in nodeSet:
        assert node.monitor.highResThroughput == 0
        assert node.monitor.totalRequests == 0

    sendReqsToNodesAndVerifySuffReplies(looper, client1, reqCount, nodeSet.f,
                                        timeout=20)

    for node in nodeSet:
        assert len(node.monitor.orderedRequestsInLast) == reqCount
        assert node.monitor.highResThroughput > 0
        assert node.monitor.totalRequests == reqCount
        count = node.monitor.spylog.count(Monitor.sendThroughput.__name__)
        if count > 0:
            assert node.monitor.hasMasterPrimary

    looper.runFor(config.DashboardUpdateFreq)

    for node in nodeSet:
        node.monitor.spylog.count(Monitor.sendThroughput.__name__) > 0

    looper.runFor(config.ThroughputWindowSize)

    def chk():
        for node in nodeSet:
            assert len(node.monitor.orderedRequestsInLast) == 0
            assert node.monitor.highResThroughput == 0
            assert node.monitor.totalRequests == reqCount

    looper.run(eventually(chk, retryWait=1, timeout=10))


def testPostingLatency(postingStatsEnabled, looper: Looper,
                          nodeSet: TestNodeSet,
                          client1):
    """
    The latencies (master as well as average of backups) after
    `DashboardUpdateFreq` seconds and before sending any requests should be zero.
    Send `n` requests in less than `LatencyWindowSize` seconds and the
    latency till `LatencyWindowSize` should consider those `n` requests.
    After `LatencyWindowSize` seconds the latencies should be zero
    """
    reqCount = 10
    for node in nodeSet:
        assert node.monitor.masterLatency == 0
        assert node.monitor.avgBackupLatency == 0

    sendReqsToNodesAndVerifySuffReplies(looper, client1, reqCount,
                                        nodeSet.f,
                                        timeout=20)

    for node in nodeSet:
        assert node.monitor.masterLatency > 0
        assert node.monitor.avgBackupLatency > 0

    looper.runFor(config.DashboardUpdateFreq)

    for node in nodeSet:
        node.monitor.spylog.count(Monitor.sendLatencies.__name__) > 0

    looper.runFor(config.ThroughputWindowSize)

    def chk():
        for node in nodeSet:
            assert node.monitor.masterLatency == 0
            assert node.monitor.avgBackupLatency == 0

    looper.run(eventually(chk, retryWait=1, timeout=10))
