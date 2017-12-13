from plenum.test.view_change.helper import view_change_in_between_3pc_random_delays
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper


TestRunningTimeLimitSec = 300


def test_view_change_in_between_3pc_all_nodes_random_delays(
        txnPoolNodeSet, tconf, looper, wallet1, client):
    """
    - Slow processing 3PC messages for all nodes randomly
    - do view change
    """
    view_change_in_between_3pc_random_delays(looper, txnPoolNodeSet,
                                             txnPoolNodeSet,
                                             wallet1, client, tconf)


def test_view_change_in_between_3pc_all_nodes_random_delays_long_delay(
        txnPoolNodeSet, looper, wallet1, client, tconf):
    """
    - Slow processing 3PC messages for all nodes randomly
    - do view change
    """
    view_change_in_between_3pc_random_delays(looper, txnPoolNodeSet,
                                             txnPoolNodeSet,
                                             wallet1, client, tconf,
                                             min_delay=5)
