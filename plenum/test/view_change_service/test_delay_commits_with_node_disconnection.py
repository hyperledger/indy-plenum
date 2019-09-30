import pytest

from plenum.test.delayers import cDelay
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import getNonPrimaryReplicas, ensureElectionsDone
from plenum.test.view_change_service.helper import get_next_primary_name, trigger_view_change


def test_view_change_with_next_primary_stopped_and_one_node_lost_commit(looper, txnPoolNodeSet,
                                                                        sdk_pool_handle, sdk_wallet_client,
                                                                        limitTestRunningTime):
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    next_primary = get_next_primary_name(txnPoolNodeSet, current_view_no + 1)
    delayed_node = [r.node for r in getNonPrimaryReplicas(txnPoolNodeSet) if r.node.name != next_primary][0]
    other_nodes = [n for n in txnPoolNodeSet if n.name != next_primary]

    with delay_rules_without_processing(delayed_node.nodeIbStasher, cDelay()):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 2)

        disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, next_primary)
        trigger_view_change(other_nodes)

    ensureElectionsDone(looper, other_nodes,
                        instances_list=range(2), customTimeout=15)
    ensure_all_nodes_have_same_data(looper, other_nodes)
    sdk_ensure_pool_functional(looper, other_nodes, sdk_wallet_client, sdk_pool_handle)
    ensure_all_nodes_have_same_data(looper, other_nodes)
