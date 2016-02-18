from functools import partial

import pytest

from zeno.common.util import getMaxFailures
from zeno.test.eventually import eventually
from zeno.test.helper import sendRandomRequest, checkSufficientRepliesRecvd, icDelay
from zeno.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing
from zeno.test.testing_utils import adict

nodeCount = 7
faultyNodes = 2
whitelist = ["discarding message"]

@pytest.fixture(scope="module")
def setup(startedNodes):
    B = startedNodes.Beta
    G = startedNodes.Gamma
    E = startedNodes.Epsilon
    for node in B, G, E:
        makeNodeFaulty(node, partial(delaysPrePrepareProcessing, delay=1, instId=0))
    return startedNodes, adict(faulties=(B, G, E))


@pytest.fixture(scope="module")
def requests(setup, looper, client1):
    fValue = getMaxFailures(nodeCount)
    requests = []
    for i in range(5):
        req = sendRandomRequest(client1)
        looper.run(eventually(checkSufficientRepliesRecvd, client1.inBox, req.reqId, fValue,
                              retryWait=1, timeout=60))
        requests.append(req)
    return setup[0], requests


def testInstChangeWithMoreReqLat(requests):
    for node in requests[0]:
        for rq in requests[1]:
            assert node.monitor.masterReqLatencies[(rq.clientId, rq.reqId)] <= node.monitor.Lambda