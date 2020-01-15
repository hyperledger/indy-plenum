import pytest

from plenum.test.delayers import icDelay, delay_for_view, vc_delay
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules
from plenum.test.test_node import checkProtocolInstanceSetup, ensureElectionsDone
from plenum.test.view_change_service.helper import trigger_view_change

nodeCount = 7


def test_catchup_to_next_view_during_view_change_0_to_1_then_1_to_2(txnPoolNodeSet, looper,
                                                                    sdk_pool_handle, sdk_wallet_steward):
    '''
    1) Lagging node is not a primary for new views
    2) All nodes except the lagging one go to view=1
    3) All nodes except the lagging one order txns on view=1
    4) All nodes except the lagging one go to view=2
    5) All nodes except the lagging one order txns on view=2
    6) Lagging node gets InstanceChanges for view=1 => it changes to view=1, and catches up till txns from view=2
    7) Lagging node gets InstanceChanges for view=2 => it changes to view=2
    8) Make sure that the lagging node is up to date, and canc participate in consensus
    '''
    lagging_node = txnPoolNodeSet[0]
    other_nodes = txnPoolNodeSet[1:]
    initial_view_no = checkViewNoForNodes(txnPoolNodeSet)
    initial_last_ordered = lagging_node.master_last_ordered_3PC

    with delay_rules(lagging_node.nodeIbStasher, icDelay(viewNo=2), vc_delay(view_no=2)):
        with delay_rules(lagging_node.nodeIbStasher, delay_for_view(viewNo=0), delay_for_view(viewNo=1)):
            # view change to viewNo=1
            trigger_view_change(txnPoolNodeSet)
            waitForViewChange(looper,
                              other_nodes,
                              expectedViewNo=initial_view_no + 1)
            checkProtocolInstanceSetup(looper=looper, nodes=other_nodes, instances=range(3))
            ensure_all_nodes_have_same_data(looper, nodes=other_nodes)

            # order some txns
            sdk_send_random_and_check(looper, txnPoolNodeSet,
                                      sdk_pool_handle, sdk_wallet_steward, 5)

            # view change to viewNo=2
            trigger_view_change(txnPoolNodeSet)
            waitForViewChange(looper,
                              other_nodes,
                              expectedViewNo=initial_view_no + 2)
            checkProtocolInstanceSetup(looper=looper, nodes=other_nodes, instances=range(3))
            ensure_all_nodes_have_same_data(looper, nodes=other_nodes)

            # order some txns
            sdk_send_random_and_check(looper, txnPoolNodeSet,
                                      sdk_pool_handle, sdk_wallet_steward, 5)

            assert initial_view_no == lagging_node.viewNo
            assert initial_last_ordered == lagging_node.master_last_ordered_3PC

        # make sure that the first View Change happened on the lagging node
        waitForViewChange(looper, [lagging_node], expectedViewNo=initial_view_no + 1,
                          customTimeout=20)
        assert initial_view_no + 1 == lagging_node.viewNo

    # make sure that the second View Change happened on the lagging node
    waitForViewChange(looper, [lagging_node], expectedViewNo=initial_view_no + 2,
                      customTimeout=20)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=other_nodes)

    # make sure that the pool is functional
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)


@pytest.mark.skip("INDY-2044")
def test_catchup_to_next_view_during_view_change_0_to_2(txnPoolNodeSet, looper,
                                                        sdk_pool_handle, sdk_wallet_steward):
    '''
    1) Lagging node is not a primary for new views
    2) All nodes except the lagging one go to view=1
    3) All nodes except the lagging one order txns on view=1
    4) All nodes except the lagging one go to view=2
    5) All nodes except the lagging one order txns on view=2
    6) Lagging node gets InstanceChanges for view=1 and view=2 => it changes to view=2, and catches up till txns from view=2
    7) Make sure that the lagging node is up to date, and can participate in consensus
    '''
    lagging_node = txnPoolNodeSet[0]
    other_nodes = txnPoolNodeSet[1:]
    initial_view_no = checkViewNoForNodes(txnPoolNodeSet)
    initial_last_ordered = lagging_node.master_last_ordered_3PC

    with delay_rules(lagging_node.nodeIbStasher, delay_for_view(viewNo=0), delay_for_view(viewNo=1),
                     delay_for_view(viewNo=2)):
        # view change to viewNo=1
        trigger_view_change(txnPoolNodeSet)
        waitForViewChange(looper,
                          other_nodes,
                          expectedViewNo=initial_view_no + 1)
        checkProtocolInstanceSetup(looper=looper, nodes=other_nodes, instances=range(3))
        ensure_all_nodes_have_same_data(looper, nodes=other_nodes)

        # order some txns
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward, 5)

        # view change to viewNo=2
        trigger_view_change(txnPoolNodeSet)
        waitForViewChange(looper,
                          other_nodes,
                          expectedViewNo=initial_view_no + 2)
        checkProtocolInstanceSetup(looper=looper, nodes=other_nodes, instances=range(3))
        ensure_all_nodes_have_same_data(looper, nodes=other_nodes)

        # order some txns
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward, 5)

        assert initial_view_no == lagging_node.viewNo
        assert initial_last_ordered == lagging_node.master_last_ordered_3PC

    # make sure that the second View Change happened on the lagging node
    waitForViewChange(looper, [lagging_node], expectedViewNo=initial_view_no + 2,
                      customTimeout=20)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=other_nodes)

    # make sure that the pool is functional
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)
