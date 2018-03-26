from plenum.test.delayers import delay_3pc_messages, vcd_delay
from plenum.test.helper import sdk_send_batches_of_random_and_check, sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    ensure_all_nodes_have_same_data
from plenum.test.test_node import getNonPrimaryReplicas
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually


def test_view_change_done_delayed(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
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
        assert node.view_changer.has_acceptable_view_change_quorum
        assert node.view_changer._primary_verified
        assert node.isParticipating
        assert None not in {r.isPrimary for r in node.replicas}

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, 5 * 4, 4)

    ensure_view_change(looper, nodes=txnPoolNodeSet)

    # After view change, the slow node successfully completes catchup
    waitNodeDataEquality(looper, slow_node, *other_nodes)

    # Other nodes complete view change, select primary and participate
    for node in other_nodes:
        looper.run(eventually(chk, node, retryWait=1))

    # Since `ViewChangeCone` is delayed, slow_node is not able to select primary
    # and participate
    assert not slow_node.view_changer.has_acceptable_view_change_quorum
    assert not slow_node.view_changer._primary_verified
    assert not slow_node.isParticipating
    assert {r.isPrimary for r in slow_node.replicas} == {None}

    # Send requests to make sure pool is functional
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)

    # Repair network
    slow_node.reset_delays_and_process_delayeds()

    # `slow_node` selects primary and participate
    looper.run(eventually(chk, slow_node, retryWait=1))

    # Processes requests received during lack of primary
    waitNodeDataEquality(looper, slow_node, *other_nodes)

    # Send more requests and compare data of all nodes
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
