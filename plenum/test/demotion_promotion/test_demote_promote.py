import pytest

from plenum.test.helper import checkViewNoForNodes, waitForViewChange, sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import demote_node, promote_node
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import restart_node

nodeCount = 6


@pytest.fixture(scope="module")
def tconf(tconf):
    old_b_size = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1

    yield tconf
    tconf.Max3PCBatchSize = old_b_size


def test_demote_promote_restart_after_promotion(txnPoolNodeSet,
                                                looper,
                                                sdk_pool_handle,
                                                sdk_wallet_stewards,
                                                tdir,
                                                tconf,
                                                allPluginsPath):
    demoted_node = txnPoolNodeSet[-1]
    steward_wallet = sdk_wallet_stewards[len(txnPoolNodeSet) - 1]
    rest_nodes = [n for n in txnPoolNodeSet if n != demoted_node]

    starting_view_no = checkViewNoForNodes(txnPoolNodeSet)

    demote_node(looper, steward_wallet, sdk_pool_handle, demoted_node)

    waitForViewChange(looper, rest_nodes, expectedViewNo=starting_view_no + 1)
    ensureElectionsDone(looper, rest_nodes)
    ensure_all_nodes_have_same_data(looper, rest_nodes)

    sdk_send_random_and_check(looper, rest_nodes, sdk_pool_handle, steward_wallet, 5)

    starting_view_no = checkViewNoForNodes(rest_nodes)
    promote_node(looper, steward_wallet, sdk_pool_handle, demoted_node)

    waitForViewChange(looper, rest_nodes, expectedViewNo=starting_view_no + 1)
    ensureElectionsDone(looper, rest_nodes)
    ensure_all_nodes_have_same_data(looper, rest_nodes)

    restart_node(looper, txnPoolNodeSet, demoted_node, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, txnPoolNodeSet)

    sdk_ensure_pool_functional(looper, txnPoolNodeSet, steward_wallet, sdk_pool_handle)
