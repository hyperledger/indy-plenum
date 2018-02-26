from functools import partial

import pytest
from stp_core.common.util import adict

from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing

nodeCount = 7
faultyNodes = 2
whitelist = ['cannot process incoming PREPARE']


@pytest.fixture(scope="module")
def setup(txnPoolNodeSet):
    A = txnPoolNodeSet[-2]
    B = txnPoolNodeSet[-1]
    for node in A, B:
        makeNodeFaulty(node,
                       partial(delaysPrePrepareProcessing, delay=60))
        # node.delaySelfNomination(10)
    return adict(faulties=(A, B))


@pytest.fixture(scope="module")
def afterElection(setup):
    for n in setup.faulties:
        for r in n.replicas:
            assert not r.isPrimary


def testNumOfSufficientPrePrepare(afterElection, preprepared1):
    pass
