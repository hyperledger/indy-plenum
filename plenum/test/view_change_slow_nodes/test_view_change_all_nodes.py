from plenum.test.view_change.helper import view_change_in_between_3pc


def test_view_change_in_between_3pc_all_nodes(txnPoolNodeSet, looper,
                                              vdr_pool_handle, vdr_wallet_client):
    """
    - Slow processing 3PC messages for all nodes
    - do view change
    """
    view_change_in_between_3pc(looper, txnPoolNodeSet, txnPoolNodeSet,
                               vdr_pool_handle,
                               vdr_wallet_client)


def test_view_change_in_between_3pc_all_nodes_long_delay(
        txnPoolNodeSet, looper, vdr_pool_handle, vdr_wallet_client):
    """
    - Slow processing 3PC messages for all nodes
    - do view change
    """
    view_change_in_between_3pc(looper, txnPoolNodeSet,
                               txnPoolNodeSet,
                               vdr_pool_handle, vdr_wallet_client,
                               slow_delay=20)
