from typing import Sequence

import pytest

from plenum.server.node import Node

nodeCount = 7


def testThroughputThreshold(nodeSet, requests):
    for node in nodeSet:  # type: Node
        masterThroughput, avgBackupThroughput = node.monitor.getThroughputs(
            node.instances.masterId)
        for r in node.replicas:
            print("{} stats: {}".format(r, repr(r.stats)))
        assert masterThroughput / avgBackupThroughput >= node.monitor.Delta


def testReqLatencyThreshold(nodeSet, requests):
    for node in nodeSet:
        for rq in requests:
            key = rq.identifier, rq.reqId
            assert key in node.monitor.masterReqLatenciesTest
            assert node.monitor.masterReqLatenciesTest[key] <= node.monitor.Lambda


def testClientLatencyThreshold(nodeSet: Sequence[Node], requests):
    rq = requests[0]
    for node in nodeSet:  # type: Node
        latc = node.monitor.getAvgLatency(
            node.instances.masterId)[rq.identifier]
        avglat = node.monitor.getAvgLatency(
            *node.instances.backupIds)[rq.identifier]
        assert latc - avglat <= node.monitor.Omega
