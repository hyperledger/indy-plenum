from typing import Iterable

from plenum.test.helper import TestNode, sendReqsToNodesAndVerifySuffReplies, \
    TestNodeSet
from plenum.common.util import getConfig

config = getConfig()


def testThroughput(postingStatsEnabled, looper, nodeSet: TestNodeSet, client1):
    reqCount = 10
    sendReqsToNodesAndVerifySuffReplies(looper, client1, reqCount, nodeSet.f,
                                        timeout=20)
    for node in nodeSet:
        assert len(node.monitor.orderedRequestsInLast) == reqCount

    looper.runFor(config.ThroughputInterval)
    sendReqsToNodesAndVerifySuffReplies(looper, client1, 1, nodeSet.f,
                                        timeout=20)
    for node in nodeSet:
        assert len(node.monitor.orderedRequestsInLast) == 1

