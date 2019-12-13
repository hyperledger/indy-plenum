from plenum.test.view_change_service.helper import check_view_change_adding_new_node


def test_view_change_while_adding_new_node_1_slow_preprepare(looper, tdir, tconf, allPluginsPath,
                                                             txnPoolNodeSet,
                                                             sdk_pool_handle,
                                                             sdk_wallet_client,
                                                             sdk_wallet_steward):
    check_view_change_adding_new_node(looper, tdir, tconf, allPluginsPath,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_client,
                                      sdk_wallet_steward,
                                      slow_nodes=[txnPoolNodeSet[1]],
                                      delay_pre_prepare=True,
                                      delay_commit=False)
