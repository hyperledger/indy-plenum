import logging

import pytest

from plenum.test.helper import checkViewNoForNodes, waitForViewChange, sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import demote_node, promote_node
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import restart_node


logging.getLogger("indy").setLevel(logging.CRITICAL)
nodeCount = 10

whitelist = ['Node is not a validator']


@pytest.fixture(scope="module")
def tconf(tconf):
    old_b_size = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1

    yield tconf
    tconf.Max3PCBatchSize = old_b_size


def test_demote_promote_restart_after_promotion_from_10_to_4_nodes(txnPoolNodeSet,
                                                                   looper,
                                                                   sdk_pool_handle,
                                                                   sdk_wallet_stewards,
                                                                   tdir,
                                                                   tconf,
                                                                   allPluginsPath):
    """
    We expect that 2 changes for f value should be happened
    """
    def demote_another_one(rest_pool):
        demoted_node = rest_pool[-1]
        wallet_steward = sdk_wallet_stewards[len(rest_pool) - 1]
        rest_pool = [n for n in rest_pool if n != demoted_node]

        starting_view_no = checkViewNoForNodes(rest_pool)

        demote_node(looper, wallet_steward, sdk_pool_handle, demoted_node)

        waitForViewChange(looper, rest_pool, expectedViewNo=starting_view_no + 1)
        ensureElectionsDone(looper, rest_pool, customTimeout=60)
        ensure_all_nodes_have_same_data(looper, rest_pool)
        return rest_pool

    rest_nodes = txnPoolNodeSet
    etalon_node = txnPoolNodeSet[-1]
    etalon_steward_wallet = sdk_wallet_stewards[len(txnPoolNodeSet) - 1]
    while len(rest_nodes) > 4:
        rest_nodes = demote_another_one(rest_nodes)
        sdk_send_random_and_check(looper, rest_nodes, sdk_pool_handle, sdk_wallet_stewards[0], 5)

    starting_view_no = checkViewNoForNodes(rest_nodes)
    promote_node(looper, etalon_steward_wallet, sdk_pool_handle, etalon_node)

    waitForViewChange(looper, rest_nodes, expectedViewNo=starting_view_no + 1)
    ensure_all_nodes_have_same_data(looper, rest_nodes)

    rest_nodes.append(etalon_node)
    restart_node(looper, rest_nodes, etalon_node, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, rest_nodes)

    sdk_ensure_pool_functional(looper, rest_nodes, sdk_wallet_stewards[0], sdk_pool_handle)
