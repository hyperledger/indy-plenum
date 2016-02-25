from typing import Sequence

import pytest

from zeno.server.node import Node
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
    for node in nodeSet:  # type: Node
        masterThroughput, avgBackupThroughput = node.monitor.getThroughputs(node.instances.masterId)
        for r in node.replicas:
            print("{} stats: {}".format(r, r.stats.__repr__()))
        assert masterThroughput / avgBackupThroughput >= node.monitor.Delta


def testReqLatencyThreshold(nodeSet, requests):
    for node in nodeSet:
        for rq in requests:
            key = rq.clientId, rq.reqId
            assert key in node.monitor.masterReqLatencies
            assert node.monitor.masterReqLatencies[key] <= node.monitor.Lambda


def testClientLatencyThreshold(nodeSet: Sequence[Node], requests):
    rq = requests[0]
    for node in nodeSet:  # type: Node
        latc = node.monitor.getAvgLatency(node.instances.masterId)[rq.clientId]
        avglat = node.monitor.getAvgLatency(*node.instances.backupIds)[rq.clientId]
        assert latc - avglat <= node.monitor.Omega
