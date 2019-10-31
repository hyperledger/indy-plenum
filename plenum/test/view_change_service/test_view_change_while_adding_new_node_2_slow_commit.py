import pytest

from plenum.test.delayers import cDelay
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import getNonPrimaryReplicas, ensureElectionsDone
from plenum.test.view_change.helper import add_new_node
from plenum.test.view_change_service.helper import get_next_primary_name, trigger_view_change, \
    check_view_change_adding_new_node


@pytest.fixture(scope="module", params=['twice', 'once'])
def vc_counts(request):
    return request.param


@pytest.fixture(scope="module", params=[True, False])
def slow_node_is_next_primary(request):
    return request.param


def test_view_change_while_adding_new_node_2_slow_commit(looper, tdir, tconf, allPluginsPath,
                                                         txnPoolNodeSet,
                                                         sdk_pool_handle,
                                                         sdk_wallet_client,
                                                         sdk_wallet_steward):
    check_view_change_adding_new_node(looper, tdir, tconf, allPluginsPath,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_client,
                                      sdk_wallet_steward,
                                      slow_nodes=[txnPoolNodeSet[1], txnPoolNodeSet[2]],
                                      delay_pre_prepare=False,
                                      delay_commit=True,
                                      expected_viewno=5)
