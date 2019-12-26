from plenum.common.constants import NEW_VIEW, STEWARD_STRING, VALIDATOR, POOL_LEDGER_ID
from plenum.common.util import randomString
from plenum.test.delayers import nv_delay, msg_rep_delay, ppgDelay
from plenum.test.helper import waitForViewChange
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import sdk_add_new_nym, sdk_add_new_node
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import ensureElectionsDone, TestNode, getPrimaryReplica
from plenum.test.view_change_service.helper import trigger_view_change
from plenum.test.view_change_with_delays.helper import check_view_change_done
from stp_core.loop.eventually import eventually


def check_node_txn_applied(nodes, old_pool_state_root_hash):
    for n in nodes:
        assert n.stateRootHash(ledgerId=POOL_LEDGER_ID, isCommitted=False) != old_pool_state_root_hash


def check_node_txn_not_applied(nodes, old_pool_state_root_hash):
    for n in nodes:
        assert n.stateRootHash(ledgerId=POOL_LEDGER_ID, isCommitted=False) == old_pool_state_root_hash


def check_node_txn_propagated(nodes):
    for n in nodes:
        master_ordering_service = n.master_replica._ordering_service
        assert len(master_ordering_service.requestQueues.get(POOL_LEDGER_ID, set())) > 0


def test_view_change_add_one_node_uncommitted_by_next_primary(looper, tdir, tconf, allPluginsPath,
                                                              txnPoolNodeSet,
                                                              sdk_pool_handle,
                                                              sdk_wallet_client,
                                                              sdk_wallet_steward):
    # 1. Pre-requisites: viewNo=2, Primary is Node3
    for viewNo in range(1, 3):
        trigger_view_change(txnPoolNodeSet)
        waitForViewChange(looper, txnPoolNodeSet, viewNo)
        ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=30)

    # 2. Add Steward for new Node
    new_steward_wallet_handle = sdk_add_new_nym(looper,
                                                sdk_pool_handle,
                                                sdk_wallet_steward,
                                                alias="testClientSteward" + randomString(3),
                                                role=STEWARD_STRING)

    # 3. Send txn to add Node5
    # It will not be proposed and ordered by the current Primary, but will be proposed by the next one in the new view
    # Make sure that the request is propagated by the next Primary
    old_state_root_hash = txnPoolNodeSet[0].stateRootHash(ledgerId=POOL_LEDGER_ID, isCommitted=False)
    primary_node = getPrimaryReplica(txnPoolNodeSet).node
    next_primary = txnPoolNodeSet[-1]
    with delay_rules_without_processing(primary_node.nodeIbStasher, ppgDelay()):
        sdk_add_new_node(
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
        looper.run(eventually(check_node_txn_propagated, [next_primary]))
        check_node_txn_not_applied(txnPoolNodeSet, old_state_root_hash)

    # 4. Trigger view change to view
    # Make sure that only the next Primary (Node4) finishes View Change to view=3
    slow_nodes = txnPoolNodeSet[:3]
    fast_nodes = [next_primary]
    slow_stashers = [slow_node.nodeIbStasher for slow_node in slow_nodes]
    with delay_rules_without_processing(slow_stashers, nv_delay(), msg_rep_delay(types_to_delay=[NEW_VIEW])):
        trigger_view_change(txnPoolNodeSet)
        waitForViewChange(looper, txnPoolNodeSet, 3)

        # view change is finished on Node4 only
        looper.run(eventually(check_view_change_done, fast_nodes, 3))
        for n in slow_nodes:
            assert n.master_replica._consensus_data.waiting_for_new_view

        # wait till fast nodes apply the Node txn in the new View (Node4 creates a new batch with it)
        looper.run(eventually(check_node_txn_applied, fast_nodes, old_state_root_hash))
        check_node_txn_not_applied(slow_nodes, old_state_root_hash)

    # 5. Trigger view change to view=4, and make sure it's finished properly
    trigger_view_change(txnPoolNodeSet)
    waitForViewChange(looper, txnPoolNodeSet, 4)
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=35)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
