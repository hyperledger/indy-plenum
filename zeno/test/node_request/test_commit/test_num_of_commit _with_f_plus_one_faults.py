from functools import partial

import pytest

from zeno.common.util import getNoInstances
from zeno.test.node_request.node_request_helper import checkCommited
from zeno.test.testing_utils import adict

from zeno.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing, \
    changesRequest

nodeCount = 7
# f + 1 faults, i.e, num of faults greater than system can tolerate
faultyNodes = 3

whitelist = ['InvalidSignature',
             'cannot process incoming PREPARE']


@pytest.fixture(scope="module")
def setup(startedNodes):
    A = startedNodes.Alpha
    B = startedNodes.Beta
    G = startedNodes.Gamma
    for node in A, B, G:
        makeNodeFaulty(node, changesRequest, partial(delaysPrePrepareProcessing,
                                                     delay=90))
        node.delaySelfNomination(10)
    return adict(faulties=(A, B, G))


@pytest.fixture(scope="module")
def afterElection(setup, up):
    for n in setup.faulties:
        for r in n.replicas:
            assert not r.isPrimary


def testNumOfCommitMsgsWithFPlusOneFaults(afterElection, looper,
                                          nodeSet, prepared1):
    with pytest.raises(AssertionError):
        checkCommited(looper,
                      nodeSet,
                      prepared1,
                      range(getNoInstances(len(nodeSet))),
                      faultyNodes)
