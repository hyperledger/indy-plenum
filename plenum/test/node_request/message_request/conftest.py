import pytest

from plenum.common.util import check_if_all_equal_in_list
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.helper import vdr_send_random_and_check


@pytest.fixture(scope="module")
def teardown(request, looper, txnPoolNodeSet, vdr_wallet_client, vdr_pool_handle):
    def tear():
        # Repair any broken network
        for node in txnPoolNodeSet:
            node.reset_delays_and_process_delayeds()
        # Give a little time to process any delayed messages
        looper.runFor(3)

        # Check each node has same data
        ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

        # Check each node has ordered all requests (no catchup)
        assert check_if_all_equal_in_list([n.master_replica._ordering_service.ordered
                                           for n in txnPoolNodeSet])

        # Check the network is functional since all nodes reply
        vdr_send_random_and_check(looper, txnPoolNodeSet, vdr_pool_handle, vdr_wallet_client, 5)

    request.addfinalizer(tear)
