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
            makeNodeFaulty(node, partial(delaysPrePrepareProcessing, delay=1,
                                         instId=0))
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


@pytest.mark.xfail(reason="Monitor stats are reset on view change. Delaying "
                          "the "
                          "request this way will drop the throughput request "
                          "latency check wont be triggered")
def testInstChangeWithMoreReqLat(looper, nodesAndRequests):
    # TODO: Set Delta to be high so that throughput check always passes but
    # latency check fails
    startedNodes, requests = nodesAndRequests
    for node in startedNodes:
        assert any(node.monitor.masterReqLatencies[(rq.clientId, rq.reqId)] >=
                   node.monitor.Lambda for rq in requests)
    looper.run(eventually(partial(checkViewNoForNodes, startedNodes, 1),
                          retryWait=1, timeout=40))


@pytest.mark.xfail(reason="Monitor stats are on view change. Delaying the "
                          "request this way will drop the throughput request "
                          "latency check wont be triggered")
def testInstChangeWithDiffGreaterThanOmega(looper, nodeSet, client1,
                                           nodesAndRequests):
    # TODO: Set Delta to be high so that throughput check always passes. Also
    # have requests from multiple clients and delay requests only from a
    # particular client and set Lambda to be high enough that test for master
    # request latency passes but test for Omega fails
    startedNodes = nodesAndRequests[0]
    instIds = range(getNoInstances(len(nodeSet)))
    masterInstId = instIds[0]
    backupInstIds = instIds[1:]
    assert any(node.monitor.getAvgLatencyForClient(client1, masterInstId) -
               node.monitor.getAvgLatencyForClient(*backupInstIds) >= node.monitor.Omega
               for node in startedNodes)
    looper.run(eventually(partial(checkViewNoForNodes, startedNodes, 1),
                          retryWait=1, timeout=40))


def testInstChangeWithLowerRatioThanDelta(looper, client1, nodeSet, nodesAndRequests):
    startedNodes = nodesAndRequests[0]
    instIds = range(getNoInstances(len(nodeSet)))
    masterInstId = instIds[0]

    for node in startedNodes:
        try:
            mast = node.monitor.getThroughput(masterInstId)
            others = node.monitor.getAvgThroughput(masterInstId)
            deltaRatio = mast / others
            assert deltaRatio <= node.monitor.Delta
        except ZeroDivisionError:
            assert node.viewNo >= 0

    looper.run(eventually(lambda: node.viewNo > 0, retryWait=1, timeout=40))
