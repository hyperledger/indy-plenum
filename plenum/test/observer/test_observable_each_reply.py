from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.spy_helpers import get_count


def test_can_process(policy_each_reply):
    assert policy_each_reply.can_process(ObserverSyncPolicyType.EACH_BATCH)
    assert not policy_each_reply.can_process("Unknown")
    assert not policy_each_reply.can_process("")
    assert not policy_each_reply.can_process(None)


def test_send_to_observers_each_reply_no_observers(observable, fake_msg_batch_committed):
    assert 0 == get_count(observable, observable.send_to_observers)
    observable.process_new_batch(fake_msg_batch_committed, "sender1")
    assert 0 == get_count(observable, observable.send_to_observers)
    assert 0 == len(observable._outbox)


def test_send_to_observers_each_reply_with_observers(observable, fake_msg_batch_committed):
    observable.add_observer("observer1", ObserverSyncPolicyType.EACH_BATCH)
    assert 0 == get_count(observable, observable.send_to_observers)
    observable.process_new_batch(fake_msg_batch_committed, "sender1")
    assert 1 == get_count(observable, observable.send_to_observers)
    assert 1 == len(observable._outbox)


def test_send_to_observers_each_reply_with_node_no_observers(node_observable,
                                                             looper,
                                                             txnPoolNodeSet,
                                                             sdk_wallet_client, sdk_pool_handle):
    assert 0 == get_count(node_observable, node_observable.send_to_observers)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client,
                              1)
    assert 0 == get_count(node_observable, node_observable.send_to_observers)
    assert 0 == len(node_observable._outbox)


def test_send_to_observers_each_reply_with_node_with_observers(node_observable,
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
