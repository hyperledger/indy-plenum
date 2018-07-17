import pytest

from plenum.test.helper import get_key_from_req

nodeCount = 7

@pytest.fixture(scope="module")
def tconf(tconf):
    old_thr_window_size = tconf.ThroughputInnerWindowSize
    old_thr_window_count = tconf.ThroughputMinActivityThreshold
    old_min_cnt = tconf.MIN_LATENCY_COUNT
    tconf.ThroughputInnerWindowSize = 5
    tconf.ThroughputMinActivityThreshold = 2
    tconf.MIN_LATENCY_COUNT = 1
    yield tconf

    tconf.MIN_LATENCY_COUNT = old_min_cnt
    tconf.ThroughputInnerWindowSize = old_thr_window_size
    tconf.ThroughputMinActivityThreshold = old_thr_window_count


def testThroughputThreshold(looper, txnPoolNodeSet, tconf, requests):
    looper.runFor(tconf.ThroughputInnerWindowSize *
                  tconf.ThroughputMinActivityThreshold)
    for node in txnPoolNodeSet:
        masterThroughput, avgBackupThroughput = node.monitor.getThroughputs(
            node.instances.masterId)
        for r in node.replicas:
            print("{} stats: {}".format(r, repr(r.stats)))
        assert masterThroughput / avgBackupThroughput >= node.monitor.Delta


def testReqLatencyThreshold(looper, txnPoolNodeSet, requests):
    for node in txnPoolNodeSet:
        for rq in requests:
            key = get_key_from_req(rq)
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
