import pytest

from plenum.common.exceptions import PoolLedgerTimeoutException
from plenum.test.helper import sdk_send_random_and_check


def test_no_client_reqs_during_view_change(txnPoolNodeSet, looper,
                                           sdk_wallet_client, sdk_pool_handle):
    for node in txnPoolNodeSet:
        node.view_changer.view_change_in_progress = True
    with pytest.raises(PoolLedgerTimeoutException):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 10)

    for node in txnPoolNodeSet:
        node.view_changer.view_change_in_progress = False
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 10)
