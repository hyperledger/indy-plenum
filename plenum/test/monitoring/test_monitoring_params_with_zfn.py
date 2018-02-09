import pytest

from plenum.test.pool_transactions.conftest import looper

nodeCount = 7


def testThroughputThreshold(looper, txnPoolNodeSet, requests):
    for node in txnPoolNodeSet:
        masterThroughput, avgBackupThroughput = node.monitor.getThroughputs(
            node.instances.masterId)
        for r in node.replicas:
            print("{} stats: {}".format(r, repr(r.stats)))
        assert masterThroughput / avgBackupThroughput >= node.monitor.Delta


def testReqLatencyThreshold(looper, txnPoolNodeSet, requests):
    for node in txnPoolNodeSet:
        for rq in requests:
            key = rq['identifier'], rq['reqId']
            assert key in node.monitor.masterReqLatenciesTest
            assert node.monitor.masterReqLatenciesTest[key] <= node.monitor.Lambda


def testClientLatencyThreshold(looper, txnPoolNodeSet, requests):
    rq = requests[0]
    for node in txnPoolNodeSet:
        latc = node.monitor.getAvgLatency(
            node.instances.masterId)[rq['identifier']]
        avglat = node.monitor.getAvgLatency(
            *node.instances.backupIds)[rq['identifier']]
        assert latc - avglat <= node.monitor.Omega
