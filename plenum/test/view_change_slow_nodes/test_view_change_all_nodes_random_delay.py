from plenum.test.view_change.helper import view_change_in_between_3pc_random_delays

TestRunningTimeLimitSec = 300


def test_view_change_in_between_3pc_all_nodes_random_delays(
        txnPoolNodeSet, tconf, looper, sdk_pool_handle, sdk_wallet_client):
    """
    - Slow processing 3PC messages for all nodes randomly
    - do view change
    """
    view_change_in_between_3pc_random_delays(looper, txnPoolNodeSet,
                                             txnPoolNodeSet,
                                             sdk_pool_handle,
                                             sdk_wallet_client, tconf)


def test_view_change_in_between_3pc_all_nodes_random_delays_long_delay(
        txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client, tconf):
    """
    - Slow processing 3PC messages for all nodes randomly
    - do view change
    """
    view_change_in_between_3pc_random_delays(looper, txnPoolNodeSet,
                                             txnPoolNodeSet,
                                             sdk_pool_handle, sdk_wallet_client, tconf,
                                             min_delay=5)
