from plenum.common.constants import NEW_VIEW, STEWARD_STRING, VALIDATOR, POOL_LEDGER_ID
from plenum.common.util import randomString
from plenum.test.delayers import nv_delay, msg_rep_delay
from plenum.test.helper import waitForViewChange
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import sdk_add_new_nym, sdk_add_new_node
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import ensureElectionsDone, TestNode
from plenum.test.view_change.helper import add_new_node
from plenum.test.view_change_service.helper import trigger_view_change
from stp_core.loop.eventually import eventually


def check_node_txn_applied(nodes, old_pool_state_root_hash):
    for n in nodes:
        assert n.stateRootHash(ledgerId=POOL_LEDGER_ID, isCommitted=False) != old_pool_state_root_hash


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

    # Add Steward for new Node
    new_steward_wallet_handle = sdk_add_new_nym(looper,
                                                sdk_pool_handle,
                                                sdk_wallet_steward,
                                                alias="testClientSteward" + randomString(3),
                                                role=STEWARD_STRING)

    with delay_rules_without_processing(slow_stashers, nv_delay(), msg_rep_delay(types_to_delay=[NEW_VIEW])):
        trigger_view_change(txnPoolNodeSet)
        waitForViewChange(looper, txnPoolNodeSet, 3)
        ensureElectionsDone(looper, fast_nodes, instances_list=[0, 1])
        assert slow_nodes[0].master_replica._consensus_data.waiting_for_new_view
        assert slow_nodes[0].master_replica._consensus_data.waiting_for_new_view

        old_state_root_hash = fast_nodes[0].stateRootHash(ledgerId=POOL_LEDGER_ID, isCommitted=False)

        # Add Node5 (it will be applied to Node4 and Node1 only)
        new_node = sdk_add_new_node(
            looper,
            sdk_pool_handle,
            new_steward_wallet_handle,
            new_node_name="Psi",
            tdir=tdir,
            tconf=tconf,
            allPluginsPath=allPluginsPath,
            autoStart=True,
            nodeClass=TestNode,
            do_post_node_creation=None,
            services=[VALIDATOR],
            wait_till_added=False)

        # wait till fast nodes apply the Node txn
        looper.run(eventually(check_node_txn_applied, fast_nodes, old_state_root_hash))
        assert slow_nodes[0].master_replica._consensus_data.waiting_for_new_view
        assert slow_nodes[0].master_replica._consensus_data.waiting_for_new_view

        # assert 3 in fast_nodes[0].write_manager.node_reg_handler.node_reg_at_beginning_of_view
        # assert 3 in fast_nodes[1].write_manager.node_reg_handler.node_reg_at_beginning_of_view

    trigger_view_change(txnPoolNodeSet)
    waitForViewChange(looper, txnPoolNodeSet, 4)
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=35)
    # sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
    assert False
