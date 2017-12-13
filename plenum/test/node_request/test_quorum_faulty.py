from functools import partial
from itertools import product

import pytest

from plenum.common.util import getNoInstances
from plenum.test.batching_3pc.helper import send_and_check
from stp_core.common.util import adict
from plenum.test import waits
from plenum.test.helper import checkRequestReturnedToNode, checkRequestNotReturnedToNode, signed_random_requests, \
    check_request_is_not_returned_to_nodes
from plenum.test.node_request.node_request_helper import checkCommitted
from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing, \
    changesRequest, delaysCommitProcessing
from stp_core.loop.eventually import eventually, eventuallyAll

nodeCount = 6
# f + 1 faults, i.e, num of faults greater than system can tolerate
faultyNodes = 2

whitelist = ['InvalidSignature']


@pytest.fixture(scope="module")
def setup(startedNodes):
    # A = startedNodes.Alpha
    # B = startedNodes.Beta
    A, B = startedNodes.nodes_by_rank[-2:]
    for node in A, B:
        makeNodeFaulty(node, changesRequest,
                       partial(delaysPrePrepareProcessing, delay=90))
        # node.delaySelfNomination(10)
    return adict(faulties=(A, B))


@pytest.fixture(scope="module")
def afterElection(setup, up):
    for n in setup.faulties:
        for r in n.replicas:
            assert not r.isPrimary


def test_6_nodes_pool_cannot_reach_quorum_with_2_faulty(afterElection, looper,
                                                        nodeSet, prepared1,
                                                        wallet1, client1):
    reqs = signed_random_requests(wallet1, 1)
    with pytest.raises(AssertionError):
        send_and_check(reqs, looper, nodeSet, client1)
    check_request_is_not_returned_to_nodes(looper, nodeSet, reqs[0])
