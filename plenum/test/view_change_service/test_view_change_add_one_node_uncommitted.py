from plenum.test.delayers import nv_delay
from plenum.test.helper import waitForViewChange
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import add_new_node
from plenum.test.view_change_service.helper import trigger_view_change
from stp_core.loop.eventually import eventually


def check_has_pre_prepares(nodes):
    for n in nodes:
        assert len(n.master_replica._consensus_data.preprepared) > 0


def test_view_change_add_one_node_uncommitted(looper, tdir, tconf, allPluginsPath,
                                              txnPoolNodeSet,
                                              sdk_pool_handle,
                                              sdk_wallet_client,
                                              sdk_wallet_steward):
    # Pre-requisites: viewNo=2, Primary is Node3
    for viewNo in range(1, 3):
        trigger_view_change(txnPoolNodeSet)
        waitForViewChange(looper, txnPoolNodeSet, viewNo)
        ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=30)

    # Make sure that only the Primaries (Master and Backup, that is Node4 and Node1) finish view change to view=3
    slow_nodes = txnPoolNodeSet[1:3]
    fast_nodes = [node for node in txnPoolNodeSet if node not in slow_nodes]
    slow_stashers = [slow_node.nodeIbStasher for slow_node in slow_nodes]

    with delay_rules_without_processing(slow_stashers, nv_delay()):
        trigger_view_change(txnPoolNodeSet)
        waitForViewChange(looper, txnPoolNodeSet, 3)
        ensureElectionsDone(looper, fast_nodes, instances_list=[0, 1])

        # Add Node5 (it will applied to Node4 and Node1 only)
        new_node = add_new_node(looper,
                                fast_nodes,
                                sdk_pool_handle,
                                sdk_wallet_steward,
                                tdir,
                                tconf,
                                allPluginsPath,
                                wait_till_added=False)
        # wait till fast nodes finish re-ordering
        looper.run(eventually(check_has_pre_prepares, fast_nodes))
        assert 3 in fast_nodes[0].write_manager.node_reg_handler.node_reg_at_beginning_of_view
        assert 3 in fast_nodes[1].write_manager.node_reg_handler.node_reg_at_beginning_of_view

    trigger_view_change(txnPoolNodeSet)
    waitForViewChange(looper, txnPoolNodeSet, 4)
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=35)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
