import pytest

from plenum.test.delayers import cDelay
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import getNonPrimaryReplicas, ensureElectionsDone
from plenum.test.view_change_service.helper import get_next_primary_name, trigger_view_change


@pytest.fixture(scope="module", params=['twice', 'once'])
def vc_counts(request):
    return request.param


@pytest.fixture(scope="module", params=[True, False])
def slow_node_is_next_primary(request):
    return request.param


def test_delay_commits_for_one_node(looper,
                                    txnPoolNodeSet,
                                    sdk_pool_handle,
                                    sdk_wallet_client,
                                    slow_node_is_next_primary,
                                    vc_counts):
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    excepted_view_no = current_view_no + 1 if vc_counts == 'once' else current_view_no + 2
    next_primary = get_next_primary_name(txnPoolNodeSet, excepted_view_no)
    pretenders = [r.node for r in getNonPrimaryReplicas(txnPoolNodeSet) if not r.isPrimary]
    if slow_node_is_next_primary:
        delayed_node = [n for n in pretenders if n.name == next_primary][0]
    else:
        delayed_node = [n for n in pretenders if n.name != next_primary][0]

    with delay_rules_without_processing(delayed_node.nodeIbStasher, cDelay()):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

        trigger_view_change(txnPoolNodeSet)
        if vc_counts == 'twice':
            for node in txnPoolNodeSet:
                node.view_changer.start_view_change(current_view_no + 2)

    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=30)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
