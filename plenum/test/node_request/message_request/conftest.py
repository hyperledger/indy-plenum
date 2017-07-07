import pytest

from plenum.common.util import check_if_all_equal_in_list
from plenum.test.helper import send_reqs_to_nodes_and_verify_all_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected


@pytest.fixture(scope="module")
def teardown(request, looper, txnPoolNodeSet, client1, wallet1):

    def tear():
        # Repair any broken network
        for node in txnPoolNodeSet:
            node.reset_delays_and_process_delayeds()
        # Give a little time to process any delayed messages
        looper.runFor(3)

        # Check each node has same data
        ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

        # Check each node has ordered all requests (no catchup)
        assert check_if_all_equal_in_list([n.master_replica.ordered
                                           for n in txnPoolNodeSet])

        # Check the network is functional since all nodes reply
        send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1, 5)

    request.addfinalizer(tear)
