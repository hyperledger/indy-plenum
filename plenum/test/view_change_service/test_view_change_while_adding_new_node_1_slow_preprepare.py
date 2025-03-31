from plenum.test.view_change_service.helper import check_view_change_adding_new_node


def test_view_change_while_adding_new_node_1_slow_preprepare(looper, tdir, tconf, allPluginsPath,
                                                             txnPoolNodeSet,
                                                             vdr_pool_handle,
                                                             vdr_wallet_client,
                                                             vdr_wallet_steward):
    check_view_change_adding_new_node(looper, tdir, tconf, allPluginsPath,
                                      txnPoolNodeSet,
                                      vdr_pool_handle,
                                      vdr_wallet_client,
                                      vdr_wallet_steward,
                                      slow_nodes=[txnPoolNodeSet[1]],
                                      delay_pre_prepare=True,
                                      delay_commit=False)
