import logging
from functools import partial

import pytest
from plenum.test.malicious_behaviors_node import makeNodeFaulty, delaysPrePrepareProcessing, \
    changesRequest
from plenum.common.util import adict
from plenum.common.log import getlogger

from plenum.test.test_node import TestNodeSet

nodeCount = 7
# f + 1 faults, i.e, num of faults greater than system can tolerate
faultyNodes = 3
whitelist = ['InvalidSignature',
             'discarding message',
             'cannot process incoming PREPARE']

logger = getlogger()


@pytest.fixture(scope="module")
def setup(startedNodes):
    A = startedNodes.Alpha
    B = startedNodes.Beta
    G = startedNodes.Gamma
    for node in A, B, G:
        makeNodeFaulty(node,
                       changesRequest,
                       partial(delaysPrePrepareProcessing, delay=60))
        node.delaySelfNomination(10)
    return adict(faulties=(A, B, G))


@pytest.fixture(scope="module")
def afterElection(setup, up):
    for n in setup.faulties:
        for r in n.replicas:
            assert not r.isPrimary


def testNumOfPrePrepareWithFPlusOneFaults(afterElection,
                                          noRetryReq,
                                          preprepared1,
                                          nodeSet):
    for n in nodeSet:
        for r in n.replicas:
            if r.isPrimary:
                logger.info("{} is primary".format(r))
