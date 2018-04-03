from functools import partial

import pytest

from plenum.common.util import getNoInstances

from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing, changesRequest
from plenum.test.node_request.node_request_helper import checkCommitted
from plenum.test.node_request.helper import nodes_by_rank

nodeCount = 6
f = 1
faultyNodes = f + 1

whitelist = ['cannot process incoming PREPARE']


@pytest.fixture(scope="module")
def evilNodes(txnPoolNodeSet):
    # Delay processing of PRE-PREPARE messages for 90
    # seconds since the timeout for checking sufficient commits is 60 seconds
    for node in nodes_by_rank(txnPoolNodeSet)[-faultyNodes:]:
        makeNodeFaulty(
            node, changesRequest, partial(
                delaysPrePrepareProcessing, delay=90))


def test_num_of_commit_msg_with_2_of_6_faulty(evilNodes, looper,
                                              txnPoolNodeSet, prepared1, noRetryReq):
    with pytest.raises(AssertionError):
        # To raise an error pass less than the actual number of faults
        checkCommitted(looper,
                       txnPoolNodeSet,
                       prepared1,
                       range(getNoInstances(len(txnPoolNodeSet))),
                       f)
