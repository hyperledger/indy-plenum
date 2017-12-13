from functools import partial

import pytest
from plenum.common.util import getNoInstances

from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing, changesRequest
from plenum.test.node_request.node_request_helper import checkPrepared

nodeCount = 6
f = 1
faultyNodes = f + 1

whitelist = ['cannot process incoming PREPARE']


@pytest.fixture(scope="module")
def evilNodes(startedNodes):
    # Delay processing of PRE-PREPARE messages for 90
    # seconds since the timeout for checking sufficient commits is 60 seconds
    for node in startedNodes.nodes_by_rank[-faultyNodes:]:
        makeNodeFaulty(
            node, changesRequest, partial(
                delaysPrePrepareProcessing, delay=90))


def test_num_of_prepare_2_of_6_faulty(evilNodes, looper,
                                      nodeSet, preprepared1, noRetryReq):
    with pytest.raises(AssertionError):
        # To raise an error pass less than the actual number of faults
        checkPrepared(looper,
                      nodeSet,
                      preprepared1,
                      range(getNoInstances(len(nodeSet))),
                      f)
