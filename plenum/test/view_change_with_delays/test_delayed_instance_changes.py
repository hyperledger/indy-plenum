from plenum.common.messages.node_messages import ViewChangeDone
from plenum.test.delayers import icDelay, nv_delay
from plenum.test.helper import waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change_service.helper import trigger_view_change

nodeCount = 7


def check_vcd_msgs(node, expected_count, expected_view_no):
    if expected_view_no not in node.msgsForFutureViews:
        raise AssertionError('no messages for expected view')
    vcd_count = sum(1 for msg in node.msgsForFutureViews[expected_view_no] if isinstance(msg[0], ViewChangeDone))
    assert expected_count == vcd_count


def check_no_ic_msgs(node, expected_view_no, nodes):
    instance_changes = node.master_replica._view_change_trigger_service._instance_changes
    assert instance_changes.has_view(expected_view_no)
    for n in nodes:
        assert not instance_changes.has_inst_chng_from(expected_view_no, n)


def test_delayed_instance_changes_after_vcd_for_next_view(looper, txnPoolNodeSet):
    '''
    A node is doing view change to view=1, while the other nodes already finished view change to view=2.
    The node receives a quorum of VCD messages for view=2 before a quorum of InstanceChange messages for view=2.
    Nevertheless, the node should not start a view change to view=2 without a quorum of InstanceChanges,
    that is it should not go to propagate primary mode since it's already in view chanage state.
    The node should eventually finish view change to view=2 once receives all VCD and IS msgs for view=2
    '''
    nodes = txnPoolNodeSet
    slow_node = nodes[-1]
    fast_nodes = [n for n in nodes if n != slow_node]
    slow_stasher = slow_node.nodeIbStasher

    # 1. DO FIRST VIEW CHANGE

    # delay VCD for the first ViewChange
    with delay_rules(slow_stasher, nv_delay()):
        # Trigger view change
        trigger_view_change(nodes)
        waitForViewChange(looper, nodes, expectedViewNo=1)

        # make sure view change is finished on all nodes except the slow one
        ensureElectionsDone(looper, fast_nodes, instances_list=range(3), customTimeout=30)

        # drop all VCD to view=1
        slow_stasher.drop_delayeds()

    # 2. DO SECOND VIEW CHANGE

    # delay Instance Changes and
    # so that the slow node receives VCD for view=2 before
    # a quorum of InstanceChanges for that view while still doing view change to view=1
    with delay_rules(slow_stasher, icDelay()):

        # Trigger view change
        trigger_view_change(nodes)
        waitForViewChange(looper, fast_nodes, expectedViewNo=2)

        # make sure view change is finished on all nodes except the slow one
        ensureElectionsDone(looper, fast_nodes, instances_list=range(3))

        # slow node is still on view=1
        assert slow_node.viewNo == 1
        assert slow_node.view_change_in_progress

        # make sure that the slow node receives VCD msgs for view=2
        # and didn't receive IS msgs for view=2
        # check_vcd_msgs(slow_node, expected_view_no=2, expected_count=len(fast_nodes), )
        check_no_ic_msgs(slow_node, 2, fast_nodes)

    # 3. RESET DELAYS AND CHECK

    waitForViewChange(looper, nodes, expectedViewNo=2)
    ensureElectionsDone(looper, nodes)
    assert not slow_node.view_change_in_progress
    ensure_all_nodes_have_same_data(looper, nodes=nodes)
