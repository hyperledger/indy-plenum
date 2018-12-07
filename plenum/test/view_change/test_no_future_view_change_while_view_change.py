from plenum.test.delayers import icDelay
from plenum.test.helper import checkViewNoForNodes
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from plenum.test.test_node import checkProtocolInstanceSetup
from plenum.test.view_change.helper import ensure_view_change


def test_no_propagated_future_view_change_while_view_change(txnPoolNodeSet, looper):
    # the last node is a lagging one, which will receive ViewChangeDone messages for future view
    viewNo = checkViewNoForNodes(txnPoolNodeSet)
    lagged_node = txnPoolNodeSet[-1]
    other_nodes = list(set(txnPoolNodeSet) - {lagged_node})

    # emulate view change in progress
    lagged_node.view_changer.view_change_in_progress = True
    old_view_no = checkViewNoForNodes([lagged_node])

    initial_vhdc = \
        lagged_node.view_changer.spylog.count(lagged_node.view_changer.process_future_view_vchd_msg.__name__)

    # delay INSTANCE CHANGE on lagged nodes, so all nodes except the lagging one finish View Change
    with delay_rules(lagged_node.nodeIbStasher, icDelay()):
        # make sure that View Change happened on all nodes but the lagging one
        ensure_view_change(looper, other_nodes)
        checkProtocolInstanceSetup(looper=looper, nodes=other_nodes, instances=range(2))
        ensure_all_nodes_have_same_data(looper, nodes=other_nodes)

        # check that lagged node recived 3 Future VCD, but didn't start new view change
        assert len(other_nodes) + initial_vhdc ==\
               lagged_node.view_changer.spylog.count(lagged_node.view_changer.process_future_view_vchd_msg.__name__)
        assert old_view_no == checkViewNoForNodes([lagged_node])
