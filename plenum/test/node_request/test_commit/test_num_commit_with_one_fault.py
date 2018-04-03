from functools import partial

import pytest

from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing

nodeCount = 4
faultyNodes = 1

whitelist = ['cannot process incoming PREPARE']


@pytest.fixture(scope="module")
def evilAlpha(txnPoolNodeSet):
    # Delay processing of PRE-PREPARE messages by Alpha for 90
    # seconds since the timeout for checking sufficient commits is 60 seconds
    makeNodeFaulty(txnPoolNodeSet[0], partial(
        delaysPrePrepareProcessing, delay=90))


def testNumOfCommitMsgsWithOneFault(evilAlpha, committed1):
    pass
