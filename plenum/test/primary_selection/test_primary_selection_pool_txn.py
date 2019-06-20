import pytest

from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import sdk_add_2_nodes
from plenum.test.primary_selection.helper import check_newly_added_nodes


def check_accepted_view_change_sent(node, nodes):
    for other_node in nodes:
        if node == other_node:
            continue
        if other_node.name in node.view_changer._view_change_done:
            assert node.view_changer._view_change_done[other_node.name] == \
                   node.view_changer._accepted_view_change_done_message


def test_primary_selection_non_genesis_node(sdk_one_node_added, looper,
                                            txnPoolNodeSet, sdk_pool_handle,
                                            sdk_wallet_steward):
    sdk_ensure_pool_functional(looper, txnPoolNodeSet,
                               sdk_wallet_steward,
                               sdk_pool_handle)


@pytest.fixture(scope='module')
def two_more_nodes_added(sdk_one_node_added, looper, txnPoolNodeSet,
                         sdk_pool_handle, sdk_wallet_steward,
                         tdir, tconf, allPluginsPath):
    # check_accepted_view_change_sent(one_node_added, txnPoolNodeSet)

    new_nodes = sdk_add_2_nodes(looper, txnPoolNodeSet, sdk_pool_handle,
                                sdk_wallet_steward,
                                tdir, tconf, allPluginsPath)

    check_newly_added_nodes(looper, txnPoolNodeSet, new_nodes)
    return new_nodes


TestRunningTimeLimitSec = 120


def test_primary_selection_increase_f(
        two_more_nodes_added,
        looper,
        txnPoolNodeSet,
        sdk_wallet_steward,
        sdk_pool_handle):
    # for n in two_more_nodes_added:
    #     check_accepted_view_change_sent(n, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)

# TODO: Add more tests to make one next primary crashed, malicious, ensure primary
# selection happens after catchup
