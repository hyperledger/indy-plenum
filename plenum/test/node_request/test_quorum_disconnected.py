import pytest

from plenum.test.batching_3pc.helper import send_and_check
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from stp_core.common.util import adict
from plenum.test.helper import signed_random_requests, \
    check_request_is_not_returned_to_nodes

nodeCount = 6
# f + 1 faults, i.e, num of faults greater than system can tolerate
faultyNodes = 2

whitelist = ['InvalidSignature']


def stop_nodes(looper, nodeSet):
    faulties = nodeSet.nodes_by_rank[-faultyNodes:]
    for node in faulties:
        for r in node.replicas:
            assert not r.isPrimary
        disconnect_node_and_ensure_disconnected(
            looper, nodeSet, node, stopNode=False)
        looper.removeProdable(node)
    return adict(faulties=faulties)


def test_6_nodes_pool_cannot_reach_quorum_with_2_disconnected(
        nodeSet, looper, client1, wallet1):
    '''
    Check that we can not reach consensus when more than n-f nodes are disconnected:
    discinnect 2 of 6 nodes
    '''
    stop_nodes(looper, nodeSet)
    reqs = signed_random_requests(wallet1, 1)
    with pytest.raises(AssertionError):
        send_and_check(reqs, looper, nodeSet, client1)
    check_request_is_not_returned_to_nodes(looper, nodeSet, reqs[0])
