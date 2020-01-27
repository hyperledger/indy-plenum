import pytest

from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected, sdk_pool_refresh
from plenum.test.test_node import get_master_primary_node, checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node
from plenum.test.view_change_service.helper import send_test_instance_change, trigger_view_change
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    old_view_change_timeout = tconf.NEW_VIEW_TIMEOUT
    tconf.NEW_VIEW_TIMEOUT = 30
    yield tconf
    tconf.NEW_VIEW_TIMEOUT = old_view_change_timeout


def test_number_txns_in_catchup_and_vc_queue_valid(looper,
                                                   txnPoolNodeSet,
                                                   tconf,
                                                   sdk_pool_handle,
                                                   sdk_wallet_steward,
                                                   tdir,
                                                   allPluginsPath):
    num_txns = 5
    master_node = get_master_primary_node(txnPoolNodeSet)
    master_node_index = txnPoolNodeSet.index(master_node)
    other_nodes = txnPoolNodeSet.copy()
    other_nodes.remove(master_node)
    old_view = master_node.viewNo
    expected_view_no = old_view + 1
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, master_node, stopNode=True)
    looper.removeProdable(master_node)
    looper.run(eventually(checkViewNoForNodes, other_nodes, expected_view_no, retryWait=1,
                          timeout=tconf.NEW_VIEW_TIMEOUT))
    sdk_pool_refresh(looper, sdk_pool_handle)
    sdk_send_random_and_check(looper, other_nodes, sdk_pool_handle, sdk_wallet_steward, num_txns)
    master_node = start_stopped_node(master_node, looper, tconf,
                                     tdir, allPluginsPath)
    txnPoolNodeSet[master_node_index] = master_node
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, master_node, *txnPoolNodeSet[-1:],
                         exclude_from_check=['check_last_ordered_3pc_backup'])
    latest_info = master_node._info_tool.info
    assert latest_info['Node_info']['Catchup_status']['Number_txns_in_catchup'][1] == num_txns
    assert latest_info['Node_info']['View_change_status']['View_No'] == expected_view_no
    for n in other_nodes:
        assert n._info_tool.info['Node_info']['View_change_status']['Last_complete_view_no'] == expected_view_no


def test_instance_change_before_vc(looper,
                                   txnPoolNodeSet,
                                   tconf,
                                   sdk_pool_handle,
                                   sdk_wallet_steward):
    master_node = get_master_primary_node(txnPoolNodeSet)
    old_view = master_node.viewNo
    expected_view_no = old_view + 1
    panic_node = txnPoolNodeSet[-1]
    send_test_instance_change(panic_node)

    def has_inst_chng_in_validator_info():
        for node in txnPoolNodeSet:
            latest_info = node._info_tool.info
            ic_queue = latest_info['Node_info']['View_change_status']['IC_queue']
            assert expected_view_no in ic_queue
            reason = ic_queue[expected_view_no]["Voters"][panic_node.name]['reason']
            assert reason == Suspicions.DEBUG_FORCE_VIEW_CHANGE.code

    looper.run(eventually(has_inst_chng_in_validator_info))

    trigger_view_change(txnPoolNodeSet)

    looper.run(eventually(checkViewNoForNodes, txnPoolNodeSet, expected_view_no, retryWait=1,
                          timeout=tconf.NEW_VIEW_TIMEOUT))
    waitNodeDataEquality(looper, master_node, *txnPoolNodeSet)

    def is_inst_chngs_cleared():
        for node in txnPoolNodeSet:
            latest_info = node._info_tool.info
            assert latest_info['Node_info']['View_change_status']['IC_queue'] == {}

    looper.run(eventually(is_inst_chngs_cleared))
