from functools import partial

import pytest

from plenum.common.util import getNoInstances
from stp_core.common.util import adict
from plenum.test.node_request.node_request_helper import checkCommitted
from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing, \
    changesRequest

nodeCount = 7
# f + 1 faults, i.e, num of faults greater than system can tolerate
faultyNodes = 3

whitelist = ['InvalidSignature',
             'cannot process incoming PREPARE']


@pytest.fixture(scope="module")
def setup(txnPoolNodeSet):
    # Making nodes faulty such that no primary is chosen
    A = txnPoolNodeSet[-3]
    B = txnPoolNodeSet[-2]
    G = txnPoolNodeSet[-1]
    for node in A, B, G:
        makeNodeFaulty(
            node, changesRequest, partial(
                delaysPrePrepareProcessing, delay=90))
        # Delaying nomination to avoid becoming primary
        # node.delaySelfNomination(10)
    return adict(faulties=(A, B, G))


@pytest.fixture(scope="module")
def afterElection(setup):
    for n in setup.faulties:
        for r in n.replicas.values():
            assert not r.isPrimary


def testNumOfCommitMsgsWithFPlusOneFaults(afterElection, looper,
                                          txnPoolNodeSet, prepared1, noRetryReq):
    with pytest.raises(AssertionError):
        # To raise an error pass less than the actual number of faults
        checkCommitted(looper,
                       txnPoolNodeSet,
                       prepared1,
                       range(getNoInstances(len(txnPoolNodeSet))),
                       faultyNodes - 1)
