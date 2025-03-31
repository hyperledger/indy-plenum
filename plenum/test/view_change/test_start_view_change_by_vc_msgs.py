from plenum.test.delayers import icDelay
from plenum.test.helper import checkViewNoForNodes
from plenum.test.node_request.helper import vdr_ensure_pool_functional
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change_service.helper import get_next_primary_name, trigger_view_change
from stp_core.loop.eventually import eventually


def test_start_view_change_by_vc_msgs(looper,
                                      txnPoolNodeSet,
                                      vdr_wallet_client,
                                      vdr_pool_handle):

    delayed_node = txnPoolNodeSet[-1]
    rest_nodes = txnPoolNodeSet[:-1]
    with delay_rules_without_processing(delayed_node.nodeIbStasher, icDelay()):
        current_view_no = checkViewNoForNodes(txnPoolNodeSet)
        trigger_view_change(txnPoolNodeSet)
        looper.run(eventually(checkViewNoForNodes, rest_nodes, current_view_no + 1))
        ensureElectionsDone(looper, txnPoolNodeSet)
    vdr_ensure_pool_functional(looper, txnPoolNodeSet, vdr_wallet_client, vdr_pool_handle)


def test_delay_IC_for_next_primary(looper,
                                   txnPoolNodeSet,
                                   vdr_pool_handle,
                                   vdr_wallet_client):
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    next_primary_name = get_next_primary_name(txnPoolNodeSet, current_view_no + 1)
    next_primary = [n for n in txnPoolNodeSet if n.name == next_primary_name][0]
    rest_nodes = list(set(txnPoolNodeSet) - {next_primary})
    with delay_rules_without_processing(next_primary.nodeIbStasher, icDelay()):
        trigger_view_change(txnPoolNodeSet)
        looper.run(eventually(checkViewNoForNodes, rest_nodes, current_view_no + 1))
        ensureElectionsDone(looper, txnPoolNodeSet)
    vdr_ensure_pool_functional(looper, txnPoolNodeSet, vdr_wallet_client, vdr_pool_handle)
    assert next_primary.master_replica.isPrimary
