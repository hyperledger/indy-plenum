import pytest
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected, sdk_pool_refresh, \
    reconnect_node_and_ensure_connected
from plenum.test.test_node import get_master_primary_node
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    old_catchup_timeout = tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE
    old_view_change_timeout = tconf.VIEW_CHANGE_TIMEOUT
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = 15
    tconf.VIEW_CHANGE_TIMEOUT = 30
    yield tconf
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = old_catchup_timeout
    tconf.VIEW_CHANGE_TIMEOUT = old_view_change_timeout


def test_number_txns_in_catchup_and_vc_queue_valid(looper,
                                                   txnPoolNodeSet,
                                                   tconf,
                                                   sdk_pool_handle,
                                                   sdk_wallet_steward):
    num_txns = 5
    master_node = get_master_primary_node(txnPoolNodeSet)
    old_view = master_node.viewNo
    expected_view_no = old_view + 1
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, master_node, stopNode=False)
    looper.run(eventually(checkViewNoForNodes, txnPoolNodeSet[1:], expected_view_no, retryWait=1,
                          timeout=tconf.VIEW_CHANGE_TIMEOUT))
    sdk_pool_refresh(looper, sdk_pool_handle)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, num_txns)
    reconnect_node_and_ensure_connected(looper, txnPoolNodeSet, master_node)
    waitNodeDataEquality(looper, master_node, *txnPoolNodeSet[-1:])
    latest_info = master_node._info_tool.info
    assert latest_info['Node_info']['Catchup_status']['Number_txns_in_catchup'][1] == num_txns
    assert latest_info['Node_info']['View_change_status']['View_No'] == expected_view_no
    node_names = [n.name for n in txnPoolNodeSet[1:]]
    for node_name in node_names:
        assert latest_info['Node_info']['View_change_status']['VCDone_queue'][node_name][0] == master_node.master_primary_name
        assert latest_info['Node_info']['View_change_status']['VCDone_queue'][node_name][1]
        assert latest_info['Node_info']['View_change_status']['Last_complete_view_no'] == expected_view_no
