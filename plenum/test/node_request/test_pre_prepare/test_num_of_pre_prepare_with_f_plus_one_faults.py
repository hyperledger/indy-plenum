from functools import partial

import pytest

from plenum.test import waits
from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing, changesRequest
from plenum.common.util import getNoInstances
from stp_core.common.util import adict
from stp_core.common.log import getlogger

from plenum.test.node_request.node_request_helper import checkPrePrepared
from plenum.test.test_node import TestNodeSet

nodeCount = 7
# f + 1 faults, i.e, num of faults greater than system can tolerate
faultyNodes = 3
whitelist = ['InvalidSignature',
             'discarding message',
             'cannot process incoming PREPARE']

logger = getlogger()

delayPrePrepareSec = 60


@pytest.fixture(scope="module")
def setup(startedNodes):
    # Making nodes faulty such that no primary is chosen
    A = startedNodes.Eta
    B = startedNodes.Gamma
    G = startedNodes.Zeta
    for node in A, B, G:
        makeNodeFaulty(
            node,
            changesRequest,
            partial(
                delaysPrePrepareProcessing,
                delay=delayPrePrepareSec))
        # Delaying nomination to avoid becoming primary
        # node.delaySelfNomination(10)
    return adict(faulties=(A, B, G))


@pytest.fixture(scope="module")
def afterElection(setup, up):
    for n in setup.faulties:
        for r in n.replicas:
            assert not r.isPrimary


@pytest.fixture(scope="module")
def preprepared1WithDelay(looper, nodeSet, propagated1, faultyNodes):
    timeouts = waits.expectedPrePrepareTime(len(nodeSet)) + delayPrePrepareSec
    checkPrePrepared(looper,
                     nodeSet,
                     propagated1,
                     range(getNoInstances(len(nodeSet))),
                     faultyNodes,
                     timeout=timeouts)


def testNumOfPrePrepareWithFPlusOneFaults(
        afterElection,
        noRetryReq,
        nodeSet,
        preprepared1WithDelay):
    for n in nodeSet:
        for r in n.replicas:
            if r.isPrimary:
                logger.info("{} is primary".format(r))
