import logging

import pytest

from stp_core.common.util import adict
from plenum.server.node import Node
from plenum.test import waits
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.malicious_behaviors_node import slow_primary
from plenum.test.test_node import getPrimaryReplica
from plenum.test.view_change.helper import provoke_and_wait_for_view_change
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually


nodeCount = 7
whitelist = ["discarding message"]

"""
We start with a 7 node consensus pool
Lets call the master instance's primary replica P
make P faulty: slow to send PRE-PREPAREs
verify that throughput has dropped
verify a view change happens
"""

@pytest.fixture
def logger():
    logger = getlogger()
    old_value = logger.getEffectiveLevel()
    logger.root.setLevel(logging.DEBUG)
    yield logger
    logger.root.setLevel(old_value)

# autouse and inject before others in all tests
pytestmark = pytest.mark.usefixtures("logger")

def latestPerfChecks(nodes):
    """
    Returns spylog entry for most recent checkPerformance executions for a set
    of nodes.
    :param nodes: an iterable of Node
    :return: a dictionary of node names to the most recent checkPerformance call
    """
    return {n.name: n.spylog.getLast(Node.checkPerformance) for n in nodes}


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

    timeout = waits.expectedPoolNextPerfCheck(nodes)
    newPerfChecks = looper.run(eventually(ensureAnotherPerfCheck,
                                          retryWait=1,
                                          timeout=timeout))
    return newPerfChecks


@pytest.fixture(scope="module")
def step1(looper, nodeSet, up, wallet1, client1):
    startedNodes = nodeSet
    """
    stand up a pool of nodes and send 5 requests to client
    """
    # the master instance has a primary replica, call it P
    P = getPrimaryReplica(startedNodes)

    requests = sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
    # profile_this(sendReqsToNodesAndVerifySuffReplies, looper, client1, 5)

    return adict(P=P,
                 nodes=startedNodes,
                 requests=requests)


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
    # have been done
    for n in step1.nodes:
        assert n.viewNo == 0

    # verify Primary is still the same
    assert getPrimaryReplica(step1.nodes) == step1.P

    step1.perfChecks = newPerfChecks
    return step1


@pytest.fixture(scope="module")
def step3(step2):
    # make P (primary replica on master) faulty, i.e., slow to send
    # PRE-PREPAREs
    slow_primary(step2.nodes, 0, 5)
    return step2


@pytest.mark.skip(reason="SOV-1123 - fails intermittently")
def testInstChangeWithLowerRatioThanDelta(looper, step3, wallet1, client1):
    # from plenum.test.test_node import ensureElectionsDone
    # ensureElectionsDone(looper, [])

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 9)
    # wait for every node to run another checkPerformance
    waitForNextPerfCheck(looper, step3.nodes, step3.perfChecks)
    provoke_and_wait_for_view_change(looper, step3.nodes, 1, wallet1, client1)
