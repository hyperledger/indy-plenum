from functools import partial

import pytest

from zeno.common.util import getMaxFailures, getNoInstances
from zeno.test.conftest import nodeSet
from zeno.test.eventually import eventually
from zeno.test.helper import sendRandomRequest, checkSufficientRepliesRecvd, checkViewNoForNodes
from zeno.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing

nodeCount = 7
whitelist = ["discarding message"]


@pytest.fixture(scope="module")
def setup(startedNodes, up):
    def isPrimaryInNode(node):
        any(replica.isPrimary for replica in node.replicas)

    count = 0
    for node in startedNodes:
        if not isPrimaryInNode(node) and count <= 3:
            count += 1
            makeNodeFaulty(node, partial(delaysPrePrepareProcessing, delay=1, instId=0))
    return startedNodes


@pytest.fixture(scope="module")
def nodesAndRequests(setup, looper, client1):
    startedNodes = setup
    fValue = getMaxFailures(nodeCount)
    requests = []
    for i in range(5):
        req = sendRandomRequest(client1)
        looper.run(eventually(checkSufficientRepliesRecvd, client1.inBox, req.reqId, fValue,
                              retryWait=1, timeout=60))
        requests.append(req)
    return startedNodes, requests


@pytest.mark.xfail(reason="Known bug in implementation")
def testInstChangeWithMoreReqLat(looper, nodesAndRequests):
    startedNodes, requests = nodesAndRequests
    for node in startedNodes:
        assert any(node.monitor.masterReqLatencies[(rq.clientId, rq.reqId)] >= node.monitor.Lambda
                   for rq in requests)
    looper.run(eventually(partial(checkViewNoForNodes, startedNodes, 1),
                          retryWait=1, timeout=40))


@pytest.mark.xfail(reason="Known bug in implementation")
def testInstChangeWithDiffGreaterThanOmega(looper, client1, nodesAndRequests):
    startedNodes = nodesAndRequests[0]
    instIds = range(getNoInstances(len(nodeSet)))
    masterInstId = instIds[0]
    backupInstIds = instIds[1:]
    assert any(node.monitor.getAvgLatencyForClient(client1, masterInstId) -
               node.monitor.getAvgLatencyForClient(*backupInstIds) >= node.monitor.Omega
               for node in startedNodes)
    looper.run(eventually(partial(checkViewNoForNodes, startedNodes, 1),
                          retryWait=1, timeout=40))


@pytest.mark.xfail(reason="Known bug in implementation")
def testInstChangeWithLowerRatioThanDelta(looper, nodesAndRequests):
    startedNodes = nodesAndRequests[0]
    instIds = range(getNoInstances(len(nodeSet)))
    masterInstId = instIds[0]
    assert any(node.monitor.getThroughput(masterInstId) /
               node.monitor.getAvgThroughput(masterInstId) <= node.monitor.Delta
               for node in startedNodes)
    looper.run(eventually(partial(checkViewNoForNodes, startedNodes, 1),
                          retryWait=1, timeout=40))
