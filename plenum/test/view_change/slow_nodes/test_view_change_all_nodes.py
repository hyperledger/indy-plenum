from plenum.test.view_change.helper import view_change_in_between_3pc


def test_view_change_in_between_3pc_all_nodes(txnPoolNodeSet, looper,
                                              sdk_pool_handle, sdk_wallet_client):
    """
    - Slow processing 3PC messages for all nodes
    - do view change
    """
    view_change_in_between_3pc(looper, txnPoolNodeSet, txnPoolNodeSet,
                               sdk_pool_handle,
                               sdk_wallet_client)


def test_view_change_in_between_3pc_all_nodes_long_delay(
        txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
    """
    - Slow processing 3PC messages for all nodes
    - do view change
    """
    view_change_in_between_3pc(looper, txnPoolNodeSet,
                               txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               slow_delay=20)
