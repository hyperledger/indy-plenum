from plenum.test.view_change.helper import view_change_in_between_3pc_random_delays

TestRunningTimeLimitSec = 300


def test_view_change_in_between_3pc_all_nodes_random_delays(
        txnPoolNodeSet, tconf, looper, wallet1, client1):
    """
    - Slow processing 3PC messages for all nodes randomly
    - do view change
    """
    view_change_in_between_3pc_random_delays(looper, txnPoolNodeSet,
                                             txnPoolNodeSet,
                                             wallet1, client1, tconf)


def test_view_change_in_between_3pc_all_nodes_random_delays_long_delay(
        txnPoolNodeSet, looper, wallet1, client1, tconf):
    """
    - Slow processing 3PC messages for all nodes randomly
    - do view change
    """
    view_change_in_between_3pc_random_delays(looper, txnPoolNodeSet,
                                             txnPoolNodeSet,
                                             wallet1, client1, tconf,
                                             min_delay=5)
