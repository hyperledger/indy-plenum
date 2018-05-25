import types

import pytest

from plenum.common.util import check_if_all_equal_in_list
from plenum.test.delayers import ppDelay
from plenum.test.helper import sdk_send_batches_of_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_request.message_request.helper import split_nodes
from plenum.test.spy_helpers import get_count

whitelist = ['does not have expected state']


def test_node_requests_missing_preprepare(looper, txnPoolNodeSet,
                                          sdk_wallet_client, sdk_pool_handle,
                                          teardown):
    """
    A node has bad network with primary and thus loses PRE-PREPARE,
    it requests PRE-PREPARE from primary once it has sufficient PREPAREs
    """
    slow_node, other_nodes, primary_node, other_non_primary_nodes = split_nodes(
        txnPoolNodeSet)

    # Delay PRE-PREPAREs by large amount simulating loss
    slow_node.nodeIbStasher.delay(ppDelay(300, 0))
    old_count_pp = get_count(slow_node.master_replica,
                             slow_node.master_replica.processPrePrepare)
    old_count_mrq = {n.name: get_count(n, n.process_message_req)
                     for n in other_nodes}
    old_count_mrp = get_count(slow_node, slow_node.process_message_rep)

    sdk_send_batches_of_random_and_check(looper,
                                         txnPoolNodeSet,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         num_reqs=15,
                                         num_batches=5)

    waitNodeDataEquality(looper, slow_node, *other_nodes)

    assert not slow_node.master_replica.requested_pre_prepares

    # `slow_node` processed PRE-PREPARE
    assert get_count(slow_node.master_replica,
                     slow_node.master_replica.processPrePrepare) > old_count_pp

    # `slow_node` did receive `MessageRep`
    assert get_count(slow_node, slow_node.process_message_rep) > old_count_mrp

    # Primary node should received `MessageReq` and other nodes shouldn't
    recv_reqs = set()
    for n in other_non_primary_nodes:
        if get_count(n, n.process_message_req) > old_count_mrq[n.name]:
            recv_reqs.add(n.name)

    assert get_count(primary_node, primary_node.process_message_req) > \
           old_count_mrq[primary_node.name]
    assert len(recv_reqs) == 0

    # All nodes including the `slow_node` ordered the same requests
    assert check_if_all_equal_in_list([n.master_replica.ordered
                                       for n in txnPoolNodeSet])