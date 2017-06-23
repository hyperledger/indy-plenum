from functools import partial
from itertools import product

import pytest

from plenum.common.util import getNoInstances, adict
from plenum.test import waits
from plenum.test.helper import checkRequestReturnedToNode, checkRequestNotReturnedToNode
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
    A = startedNodes.Alpha
    B = startedNodes.Beta
    for node in A, B:
        makeNodeFaulty(node, changesRequest,
                       partial(delaysPrePrepareProcessing, delay=90))
        node.delaySelfNomination(10)
    return adict(faulties=(A, B))


@pytest.fixture(scope="module")
def afterElection(setup, up):
    for n in setup.faulties:
        for r in n.replicas:
            assert not r.isPrimary


def check_request_is_not_returned_to_nodes(looper, nodeSet, request):
    instances = range(getNoInstances(len(nodeSet)))
    coros = []
    for node, inst_id in product(nodeSet, instances):
        c = partial(checkRequestNotReturnedToNode,
                    node=node,
                    identifier=request.identifier,
                    reqId=request.reqId,
                    instId=inst_id
                    )
        coros.append(c)
    timeout = waits.expectedTransactionExecutionTime(len(nodeSet))
    looper.run(eventuallyAll(*coros, retryWait=1, totalTimeout=timeout))


def test_6_nodes_pool_cannot_reach_quorum_with_2_faulty(afterElection, looper,
                                                        nodeSet, prepared1,
                                                        noRetryReq):

    check_request_is_not_returned_to_nodes(looper, nodeSet, prepared1)



