from plenum.test.helper import sdk_send_random_and_check


def test_restart_clientstack_after_client_limit(looper,
                                                txnPoolNodeSet,
                                                sdk_pool_handle,
                                                sdk_wallet_steward):
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 10)
    looper.runFor(20)
    a = 1