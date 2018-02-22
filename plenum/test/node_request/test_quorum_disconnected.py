import pytest
import json

from plenum.test.node_request.helper import nodes_by_rank
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from stp_core.common.util import adict
from plenum.test.helper import check_request_is_not_returned_to_nodes, \
    sdk_send_and_check, sdk_json_to_request_object
from plenum.test.helper import sdk_signed_random_requests

nodeCount = 6
# f + 1 faults, i.e, num of faults greater than system can tolerate
faultyNodes = 2

whitelist = ['InvalidSignature']


def stop_nodes(looper, txnPoolNodeSet):
    faulties = nodes_by_rank(txnPoolNodeSet)[-faultyNodes:]
    for node in faulties:
        for r in node.replicas:
            assert not r.isPrimary
        disconnect_node_and_ensure_disconnected(
            looper, txnPoolNodeSet, node, stopNode=False)
        looper.removeProdable(node)
    return adict(faulties=faulties)


def test_6_nodes_pool_cannot_reach_quorum_with_2_disconnected(
        txnPoolNodeSet, looper, sdk_pool_handle,
        sdk_wallet_client):
    '''
    Check that we can not reach consensus when more than n-f nodes are disconnected:
    disconnect 2 of 6 nodes
    '''
    stop_nodes(looper, txnPoolNodeSet)
    reqs = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    with pytest.raises(AssertionError):
        sdk_send_and_check(reqs, looper, txnPoolNodeSet, sdk_pool_handle)
    check_request_is_not_returned_to_nodes(
        txnPoolNodeSet, sdk_json_to_request_object(json.loads(reqs[0])))
