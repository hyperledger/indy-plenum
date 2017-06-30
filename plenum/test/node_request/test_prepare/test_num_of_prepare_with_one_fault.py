from functools import partial

import pytest

from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing
from stp_core.common.util import adict

nodeCount = 4
faultyNodes = 1
whitelist = ['cannot process incoming PREPARE']


@pytest.fixture(scope="module")
def setup(startedNodes):
    # Making nodes faulty such that no primary is chosen
    G = startedNodes.Gamma
    # Delaying nomination to avoid becoming primary
    # G.delaySelfNomination(10)
    makeNodeFaulty(G,
                   partial(delaysPrePrepareProcessing, delay=60))
    return adict(faulty=G)


@pytest.fixture(scope="module")
def afterElection(setup, up):
    assert not setup.faulty.hasPrimary


def testNumOfPrepareWithOneFault(afterElection, prepared1):
    pass
