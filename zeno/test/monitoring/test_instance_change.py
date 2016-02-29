import logging
from functools import partial

import pytest

from zeno.common.request_types import PrePrepare
from zeno.common.util import getMaxFailures, getNoInstances
from zeno.server.node import Node
from zeno.test.eventually import eventually
from zeno.test.helper import sendRandomRequest, checkSufficientRepliesRecvd, checkViewNoForNodes, \
    getPrimaryReplica, sendReqsToNodesAndVerifySuffReplies, getAllArgs
from zeno.test.profiler import profile_this
from zeno.test.testing_utils import adict

nodeCount = 7
whitelist = ["discarding message"]

"""
We start with a 7 node consensus pool
7 nodes, means we must have an f value of 2
An f value of 2 means we must have 3 instances:
    1 master instance
    2 backup instances
the master instance has a primary replica, call it P
verify P is performing well
make P faulty: slow to send PRE-PREPAREs
verify that throughput has dropped
verify a view change happens
"""


logging.root.setLevel(logging.INFO)


@pytest.fixture(scope="module")
def step1(looper, startedNodes, up, client1):
    """
    stand up a pool of nodes and send 5 requests to client
    """
    # the master instance has a primary replica, call it P
    P = getPrimaryReplica(startedNodes)

    sendReqsToNodesAndVerifySuffReplies(looper, client1, 5)
    # profile_this(sendReqsToNodesAndVerifySuffReplies, looper, client1, 5)

    return adict(P=P,
                 nodes=startedNodes)


@pytest.fixture(scope="module")
def step2(step1, looper):
    """
    Sends requests to client and check the ratio of throughput of master
    instance and backup instance must be greater than or equal to Delta and
    verify no view change takes place.
    """
    # record when Node.checkPerformance was last run
    lastPerfChecks = latestPerfChecks(step1.nodes)

    # wait for every node to run another checkPerformance
    newPerfChecks = waitForNextPerfCheck(looper, step1.nodes, lastPerfChecks)

    # verify all nodes say that P is performing OK, and that no view changes
    # have been proposed
    # TODO This is failing intermittently. Our thresholds may be too stringent.
    for n in step1.nodes:
        assert n.viewNo == 0
        assert newPerfChecks[n.name].result  # True: no view change proposed

    # verify Primary is still the same
    # TODO This is failing intermittently. Our thresholds may be too stringent.
    assert getPrimaryReplica(step1.nodes) == step1.P

    step1.perfChecks = newPerfChecks
    return step1


def latestPerfChecks(nodes):
    """
    Returns spylog entry for most recent checkPerformance executions for a set
    of nodes.
    :param nodes: an iterable of Node
    :return: a dictionary of node names to the most recent checkPerformance call
    """
    return {n.name: n.spylog.getLast(Node.checkPerformance.__name__)
            for n in nodes}


def waitForNextPerfCheck(looper, nodes, previousPerfChecks):
    def ensureAnotherPerfCheck():
        # ensure all nodes have run another performance check
        cur = latestPerfChecks(nodes)
        for c in cur:
            if previousPerfChecks[c] is None:
                assert cur[c] is not None
            else:
                assert cur[c].endtime > previousPerfChecks[c].endtime
        return cur

    perfCheckFreq = max(n.perfCheckFreq for n in nodes)

    newPerfChecks = looper.run(eventually(ensureAnotherPerfCheck,
                                          retryWait=1,
                                          timeout=perfCheckFreq + 1))
    return newPerfChecks


def testStep2(step2):
    print("Done")


@pytest.fixture(scope="module")
def step3(step2):

    # make P (primary replica on master) faulty, i.e., slow to send PRE-PREPAREs
    def by3IfPrePrepare(msg):
        if isinstance(msg, PrePrepare):
            return 3

    step2.P.outBoxTestStasher.delay(by3IfPrePrepare)
    # send requests to client
    return step2


def testInstChangeWithLowerRatioThanDelta(looper, step3, client1):

    sendReqsToNodesAndVerifySuffReplies(looper, client1, 5)

    # wait for every node to run another checkPerformance
    newPerfChecks = waitForNextPerfCheck(looper, step3.nodes, step3.perfChecks)

    # verify all nodes recognize P as degraded
    # for n in step3.nodes:
    #     assert newPerfChecks[n.name].result is False

    # verify all nodes have undergone an instance change
    for n in step3.nodes:
        assert n.viewNo == 1


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
