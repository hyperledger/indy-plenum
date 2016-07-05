from time import sleep, time
from typing import Iterable

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
    The throughput `DashboardUpdateFreq` after seconds and before sending any
    requests should be zero.
    Send `n` requests in less than `ThroughputWindowSize` seconds and the
    throughput till `ThroughputWindowSize` should consider those `n` requests.
    After `ThroughputWindowSize` seconds the throughput should be zero
    Test `totalRequests` too.
    """
    reqCount = 20
    for node in nodeSet:
        assert node.monitor.highResThroughput == 0
        assert node.monitor.totalRequests == 0

    sendReqsToNodesAndVerifySuffReplies(looper, client1, reqCount, nodeSet.f,
                                        timeout=20)
    for node in nodeSet:
        assert len(node.monitor.orderedRequestsInLast) == reqCount
        assert node.monitor.highResThroughput > 0
        assert node.monitor.totalRequests == reqCount

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
