import pytest

from zeno.test.eventually import eventually
from zeno.test.helper import checkSufficientRepliesRecvd, sendRandomRequest

nodeCount = 4


@pytest.fixture(scope="module")
def requests(looper, client1):
    requests = []
    for i in range(5):
        req = sendRandomRequest(client1)
        looper.run(eventually(checkSufficientRepliesRecvd, client1.inBox, req.reqId, 1,
                              retryWait=1, timeout=5))
        requests.append(req)
    return requests


def testThroughtputThreshold(nodeSet, requests):
    for node in nodeSet:
        masterThroughput, avgBackupThroughput = node.monitor.getThroughputs(node.masterInst)
        for r in node.replicas:
            print("{} stats: {}".format(r, r.stats.__repr__()))
        assert masterThroughput / avgBackupThroughput >= node.monitor.Delta


def testReqLatencyThreshold(nodeSet, requests):
    for node in nodeSet:
        for rq in requests:
            assert node.monitor.masterReqLatencies[(rq.clientId, rq.reqId)] <= node.monitor.Lambda


def testClientLatencyThreshold(nodeSet, requests):
    rq = requests[0]
    for node in nodeSet:
        latc = node.monitor.getAvgLatency(node.masterInst)[rq.clientId]
        avglat = node.monitor.getAvgLatency(*node.nonMasterInsts)[rq.clientId]
        assert latc - avglat <= node.monitor.Omega
