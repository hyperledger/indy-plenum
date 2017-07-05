from functools import partial

import pytest
from stp_core.common.util import adict

from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing

nodeCount = 7
faultyNodes = 2
whitelist = ['cannot process incoming PREPARE']


@pytest.fixture(scope="module")
def setup(startedNodes):
    # Making nodes faulty such that no primary is chosen
    A = startedNodes.Gamma
    B = startedNodes.Zeta
    # Delay processing of PRE-PREPARE messages by Alpha and Beta for 90
    # seconds since the timeout for checking sufficient commits is 60 seconds
    makeNodeFaulty(A, partial(delaysPrePrepareProcessing, delay=90))
    makeNodeFaulty(B, partial(delaysPrePrepareProcessing, delay=90))
    # Delaying nomination to avoid becoming primary
    # A.delaySelfNomination(10)
    # B.delaySelfNomination(10)
    return adict(faulties=(A, B))


@pytest.fixture(scope="module")
def afterElection(setup, up):
    for n in setup.faulties:
        for r in n.replicas:
            assert not r.isPrimary


def testNumOfSufficientCommitMsgs(afterElection, committed1):
    pass
