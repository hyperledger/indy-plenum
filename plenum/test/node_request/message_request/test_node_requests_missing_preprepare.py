from plenum.common.util import check_if_all_equal_in_list
from plenum.test.delayers import ppDelay
from plenum.test.helper import send_reqs_batches_and_get_suff_replies, \
    send_reqs_to_nodes_and_verify_all_replies
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.spy_helpers import get_count
from plenum.test.test_node import getNonPrimaryReplicas, get_master_primary_node


def test_node_requests_missing_preprepare(looper, txnPoolNodeSet, client1,
                                          wallet1, client1Connected):
    """
    A node has bad network with primary and thus loses PRE-PREPARE,
    it requests PRE-PREPARE from non-primaries once it has sufficient PREPAREs
    """
    slow_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1].node
    other_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    primary_node = get_master_primary_node(txnPoolNodeSet)

    # Delay PRE-PREPAREs by large amount simulating loss
    slow_node.nodeIbStasher.delay(ppDelay(300, 0))
    old_count_pp = get_count(slow_node.master_replica,
                          slow_node.master_replica.processPrePrepare)
    old_count_mrq = {n.name: get_count(n, n.process_message_req)
                     for n in other_nodes}
    old_count_mrp = get_count(slow_node, slow_node.process_message_rep)

    send_reqs_batches_and_get_suff_replies(looper, wallet1, client1, 15, 5)
    waitNodeDataEquality(looper, slow_node, *other_nodes)

    # `slow_node` processed PRE-PREPARE
    assert get_count(slow_node.master_replica,
                     slow_node.master_replica.processPrePrepare) > old_count_pp

    # `slow_node` did receive `MessageRep`
    assert get_count(slow_node, slow_node.process_message_rep) > old_count_mrp

    # More than `f` nodes received `MessageReq`
    recv_reqs = set()
    for n in other_nodes:
        if get_count(n, n.process_message_req) > old_count_mrq[n.name]:
            recv_reqs.add(n.name)

    assert len(recv_reqs) > slow_node.f

    # All nodes including the `slow_node` ordered the same requests
    assert check_if_all_equal_in_list([n.master_replica.ordered
                                       for n in txnPoolNodeSet])

    # Repair network
    slow_node.reset_delays_and_process_delayeds()
    looper.runFor(3)
    # `slow_node` still has same data as otheres
    waitNodeDataEquality(looper, slow_node, *other_nodes)
    assert check_if_all_equal_in_list([n.master_replica.ordered
                                       for n in txnPoolNodeSet])

    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1, 5)
