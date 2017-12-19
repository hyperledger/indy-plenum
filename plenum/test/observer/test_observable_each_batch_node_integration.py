from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.spy_helpers import get_count


def test_send_to_observers_each_reply_no_observers(node_observable,
                                                   looper,
                                                   txnPoolNodeSet,
                                                   sdk_wallet_client, sdk_pool_handle):
    assert 0 == get_count(node_observable, node_observable.send_to_observers)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client,
                              1)
    assert 0 == get_count(node_observable, node_observable.send_to_observers)
    assert 0 == len(node_observable._outbox)


def test_send_to_observers_each_reply_with_observers(node_observable,
                                                     looper,
                                                     txnPoolNodeSet,
                                                     sdk_wallet_client, sdk_pool_handle):
    node_observable.add_observer("observer1", ObserverSyncPolicyType.EACH_BATCH)
    assert 0 == get_count(node_observable, node_observable.send_to_observers)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client,
                              1)
    assert 1 == get_count(node_observable, node_observable.send_to_observers)
    assert 1 == len(node_observable._outbox)
