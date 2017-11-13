from plenum.test.delayers import delay_3pc_messages, vcd_delay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    send_reqs_batches_and_get_suff_replies
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper
from plenum.test.test_node import getNonPrimaryReplicas
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually


def test_view_change_done_delayed(txnPoolNodeSet, looper, wallet1, client1,
                                  client1Connected):
    """
    A node is slow so is behind other nodes, after view change, it catches up
    but it also gets view change message as delayed, a node should start
    participating only when caught up and ViewChangeCone quorum received.
    """
    nprs = [r.node for r in getNonPrimaryReplicas(txnPoolNodeSet, 0)]
    slow_node = nprs[-1]
    other_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    delay_3pc = 10
    delay_vcd = 25
    delay_3pc_messages([slow_node], 0, delay_3pc)
    slow_node.nodeIbStasher.delay(vcd_delay(delay_vcd))

    def chk(node):
        assert node.elector.has_acceptable_view_change_quorum
        assert node.elector._primary_verified
        assert node.isParticipating
        assert None not in {r.isPrimary for r in node.replicas}

    send_reqs_batches_and_get_suff_replies(looper, wallet1, client1, 5 * 4, 4)

    ensure_view_change(looper, nodes=txnPoolNodeSet)

    # After view change, the slow node successfully completes catchup
    waitNodeDataEquality(looper, slow_node, *other_nodes)

    # Other nodes complete view change, select primary and participate
    for node in other_nodes:
        looper.run(eventually(chk, node, retryWait=1))

    # Since `ViewChangeCone` is delayed, slow_node is not able to select primary
    # and participate
    assert not slow_node.elector.has_acceptable_view_change_quorum
    assert not slow_node.elector._primary_verified
    assert not slow_node.isParticipating
    assert {r.isPrimary for r in slow_node.replicas} == {None}

    # Send requests to make sure pool is functional
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)

    # Repair network
    slow_node.reset_delays_and_process_delayeds()

    # `slow_node` selects primary and participate
    looper.run(eventually(chk, slow_node, retryWait=1))

    # Processes requests received during lack of primary
    waitNodeDataEquality(looper, slow_node, *other_nodes)

    # Send more requests and compare data of all nodes
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
