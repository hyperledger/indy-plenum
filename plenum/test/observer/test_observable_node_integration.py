from plenum.test.helper import sdk_send_random_and_check
from plenum.test.spy_helpers import get_count


def test_append_input(node_observable,
                      looper,
                      txnPoolNodeSet,
                      sdk_wallet_client, sdk_pool_handle):
    assert 0 == get_count(node_observable, node_observable.append_input)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client,
                              1)
    assert 1 == get_count(node_observable, node_observable.append_input)


def test_process_new_batch(node_observable,
                           looper,
                           txnPoolNodeSet,
                           sdk_wallet_client, sdk_pool_handle):
    called_before = get_count(node_observable, node_observable.process_new_batch)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client,
                              1)
    assert called_before + 1 == get_count(node_observable, node_observable.process_new_batch)
