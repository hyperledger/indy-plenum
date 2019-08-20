from functools import partial

import pytest
from stp_core.common.util import adict

from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing

nodeCount = 4
faultyNodes = 1
whitelist = ['cannot process incoming PREPARE']


@pytest.fixture(scope="module")
def setup(txnPoolNodeSet):
    A = txnPoolNodeSet[-1]
    makeNodeFaulty(A,
                   partial(delaysPrePrepareProcessing, delay=60))
    return adict(faulties=A)


@pytest.fixture(scope="module")
def afterElection(setup):
    for r in setup.faulties.replicas.values():
        assert not r.isPrimary


def testNumOfPrePrepareWithOneFault(afterElection, preprepared1):
    pass
