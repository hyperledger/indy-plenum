from functools import partial

import pytest
import json

from plenum.test.node_request.helper import nodes_by_rank
from stp_core.common.util import adict
from plenum.test.helper import check_request_is_not_returned_to_nodes, \
    sdk_send_and_check, sdk_json_to_request_object, sdk_signed_random_requests
from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    delaysPrePrepareProcessing, \
    changesRequest

nodeCount = 6
# f + 1 faults, i.e, num of faults greater than system can tolerate
faultyNodes = 2

whitelist = ['InvalidSignature']


@pytest.fixture(scope="module")
def setup(txnPoolNodeSet):
    # A = startedNodes.Alpha
    # B = startedNodes.Beta
    A, B = nodes_by_rank(txnPoolNodeSet)[-2:]
    for node in A, B:
        makeNodeFaulty(node, changesRequest,
                       partial(delaysPrePrepareProcessing, delay=90))
        # node.delaySelfNomination(10)
    return adict(faulties=(A, B))


@pytest.fixture(scope="module")
def afterElection(setup):
    for n in setup.faulties:
        for r in n.replicas:
            assert not r.isPrimary


def test_6_nodes_pool_cannot_reach_quorum_with_2_faulty(afterElection, looper,
                                                        txnPoolNodeSet, prepared1,
                                                        sdk_wallet_client, sdk_pool_handle):
    reqs = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    with pytest.raises(TimeoutError):
        sdk_send_and_check(reqs, looper, txnPoolNodeSet, sdk_pool_handle)
    check_request_is_not_returned_to_nodes(
        txnPoolNodeSet, sdk_json_to_request_object(json.loads(reqs[0])))
