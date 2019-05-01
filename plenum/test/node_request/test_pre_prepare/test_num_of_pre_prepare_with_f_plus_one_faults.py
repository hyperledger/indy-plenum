from functools import partial

import pytest

from plenum.test import waits
from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing, changesRequest
from plenum.common.util import getNoInstances
from stp_core.common.util import adict
from stp_core.common.log import getlogger

from plenum.test.node_request.node_request_helper import checkPrePrepared

nodeCount = 7
# f + 1 faults, i.e, num of faults greater than system can tolerate
faultyNodes = 3
whitelist = ['InvalidSignature',
             'discarding message',
             'cannot process incoming PREPARE']

logger = getlogger()

delayPrePrepareSec = 60


@pytest.fixture(scope="module")
def setup(txnPoolNodeSet):
    # Making nodes faulty such that no primary is chosen
    A = txnPoolNodeSet[-3]
    B = txnPoolNodeSet[-2]
    G = txnPoolNodeSet[-1]
    for node in A, B, G:
        makeNodeFaulty(
            node,
            changesRequest,
            partial(
                delaysPrePrepareProcessing,
                delay=delayPrePrepareSec))
        # Delaying nomination to avoid becoming primary
        # node.delaySelfNomination(10)
    return adict(faulties=(A, B, G))


@pytest.fixture(scope="module")
def afterElection(setup):
    for n in setup.faulties:
        for r in n.replicas.values():
            assert not r.isPrimary


@pytest.fixture(scope="module")
def preprepared1WithDelay(looper, txnPoolNodeSet, propagated1, faultyNodes):
    timeouts = waits.expectedPrePrepareTime(len(txnPoolNodeSet)) + delayPrePrepareSec
    checkPrePrepared(looper,
                     txnPoolNodeSet,
                     propagated1,
                     range(getNoInstances(len(txnPoolNodeSet))),
                     faultyNodes,
                     timeout=timeouts)


def testNumOfPrePrepareWithFPlusOneFaults(
        afterElection,
        noRetryReq,
        txnPoolNodeSet,
        preprepared1WithDelay):
    for n in txnPoolNodeSet:
        for r in n.replicas.values():
            if r.isPrimary:
                logger.info("{} is primary".format(r))
