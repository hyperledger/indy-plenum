from functools import partial

import pytest

from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing
from plenum.common.util import adict

nodeCount = 4
faultyNodes = 1
whitelist = ['cannot process incoming PREPARE']


@pytest.fixture(scope="module")
def setup(startedNodes):
    A = startedNodes.Alpha
    A.delaySelfNomination(10)
    makeNodeFaulty(A,
                   partial(delaysPrePrepareProcessing, delay=60))
    return adict(faulty=A)


@pytest.fixture(scope="module")
def afterElection(setup, up):
    assert not setup.faulty.hasPrimary


def testNumOfPrepareWithOneFault(afterElection, prepared1):
    pass
