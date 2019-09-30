from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check, assertExp
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected


from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change_service.helper import trigger_view_change, \
    get_next_primary_name

REQ_COUNT = 10


def test_view_change_triggered(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)

    trigger_view_change(txnPoolNodeSet)

    ensureElectionsDone(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
    assert checkViewNoForNodes(txnPoolNodeSet) == current_view_no + 1


def test_view_change_triggered_after_ordering(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, REQ_COUNT)
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)

    trigger_view_change(txnPoolNodeSet)

    ensureElectionsDone(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
    assert checkViewNoForNodes(txnPoolNodeSet) == current_view_no + 1


def test_view_change_with_next_primary_stopped(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    next_primary = get_next_primary_name(txnPoolNodeSet, current_view_no + 1)
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, next_primary)
    remaining_nodes = [node for node in txnPoolNodeSet if node.name != next_primary]

    trigger_view_change(remaining_nodes)

    ensureElectionsDone(looper, remaining_nodes, instances_list=range(2), customTimeout=15)
    sdk_ensure_pool_functional(looper, remaining_nodes, sdk_wallet_client, sdk_pool_handle)
    assert checkViewNoForNodes(remaining_nodes) == current_view_no + 2
