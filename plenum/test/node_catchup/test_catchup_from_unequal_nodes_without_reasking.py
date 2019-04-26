import pytest

from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import delay_3pc
from plenum.test.helper import sdk_send_random_and_check, max_3pc_batch_limits
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules_without_processing

nodeCount = 7


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        old_catchup_txn_timeout = tconf.CatchupTransactionsTimeout
        old_catchup_batch_size = tconf.CATCHUP_BATCH_SIZE

        # Effectively disable resending CATCHUP_REQs, so if node sends some requests that
        # cannot be processed catchup won't finish
        tconf.CatchupTransactionsTimeout = 1000

        # Make catchup batch size small to increase probability of hitting all nodes
        # with catchup requests
        tconf.CATCHUP_BATCH_SIZE = 1

        yield tconf
        tconf.CatchupTransactionsTimeout = old_catchup_txn_timeout
        tconf.CATCHUP_BATCH_SIZE = old_catchup_batch_size


def test_catchup_from_unequal_nodes_without_reasking(looper,
                                                     txnPoolNodeSet,
                                                     sdk_pool_handle,
                                                     sdk_wallet_client):
    # Node numbers are selected so that test fails when using old algo
    lagged_node_1 = txnPoolNodeSet[2]
    lagged_node_2 = txnPoolNodeSet[-1]
    with delay_rules_without_processing(lagged_node_1.nodeIbStasher, delay_3pc()):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 2)

        with delay_rules_without_processing(lagged_node_2.nodeIbStasher, delay_3pc()):
            sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 7)
            ensure_all_nodes_have_same_data(looper, set(txnPoolNodeSet) - {lagged_node_2, lagged_node_1},
                                            custom_timeout=30)

            # Perform catchup and make sure it completes successfully
            lagged_node_2.start_catchup()
            ensure_all_nodes_have_same_data(looper, set(txnPoolNodeSet) - {lagged_node_1},
                                            custom_timeout=30)
            assert lagged_node_2.ledgerManager._node_leecher._state == NodeLeecherService.State.Idle
