import pytest

from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change_service.helper import trigger_view_change, trigger_view_change_on_node, \
    get_next_primary_name

from stp_core.common.log import Logger


Logger().enableStdLogging()
Logger().setLogLevel(0)


REQ_COUNT = 10


def test_view_change_triggered(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    trigger_view_change(txnPoolNodeSet, current_view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)


def test_view_change_triggered_after_ordering(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, REQ_COUNT)
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    trigger_view_change(txnPoolNodeSet, current_view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)


@pytest.mark.skip(reason="not working now")
def test_view_change_with_next_primary_stopped(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    next_primary = get_next_primary_name(txnPoolNodeSet, old_view_no + 1)
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, next_primary)
    trigger_view_change(txnPoolNodeSet, old_view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    assert current_view_no == old_view_no + 2
