import logging
from functools import partial

import pytest
from zeno.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing
from zeno.test.testing_utils import adict

from zeno.test.helper import TestNodeSet

nodeCount = 7
faultyNodes = 2
whitelist = ['cannot process incoming PREPARE']


@pytest.fixture(scope="module")
def setup(startedNodes):
    A = startedNodes.Alpha
    B = startedNodes.Beta
    for node in A, B:
        makeNodeFaulty(node,
                       partial(delaysPrePrepareProcessing, delay=60))
        node.delaySelfNomination(10)
    return adict(faulties=(A, B))


@pytest.fixture(scope="module")
def afterElection(setup, up):
    for n in setup.faulties:
        for r in n.replicas:
            assert not r.isPrimary


def testNumOfSufficientPrepare(afterElection, prepared1, nodeSet: TestNodeSet):
    for n in nodeSet:
        for r in n.replicas:
            if r.isPrimary:
                logging.info("{} is primary".format(r))
