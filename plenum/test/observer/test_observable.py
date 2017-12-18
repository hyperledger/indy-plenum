from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.spy_helpers import get_count


def test_new_observable(observable):
    assert observable
    assert observable._get_policy(ObserverSyncPolicyType.EACH_BATCH)


def test_add_valid_observer(observable, observers):
    assert len(observers) == 0

    observable.add_observer("observer1", ObserverSyncPolicyType.EACH_BATCH)
    assert len(observers) == 1
    assert observers[0] == "observer1"

    observable.add_observer("observer2", ObserverSyncPolicyType.EACH_BATCH)
    assert len(observers) == 2
    assert observers[0] == "observer1"
    assert observers[1] == "observer2"


def test_add_invalid_observer(observable, observers):
    assert len(observers) == 0

    observable.add_observer("observer1", "unknown_policy")
    assert len(observers) == 0

    observable.add_observer("observer1", "")
    assert len(observers) == 0

    observable.add_observer("observer1", None)
    assert len(observers) == 0


def test_remove_valid_observer(observable, observers):
    observable.add_observer("observer1", ObserverSyncPolicyType.EACH_BATCH)
    observable.add_observer("observer2", ObserverSyncPolicyType.EACH_BATCH)
    assert len(observers) == 2

    observable.remove_observer("observer1")
    assert len(observers) == 1
    assert observers[0] == "observer2"

    observable.remove_observer("observer2")
    assert len(observers) == 0


def test_remove_invalid_observer(observable, observers):
    observable.add_observer("observer1", ObserverSyncPolicyType.EACH_BATCH)
    observable.add_observer("observer2", ObserverSyncPolicyType.EACH_BATCH)
    assert len(observers) == 2

    observable.remove_observer("observer3")
    assert len(observers) == 2

    observable.remove_observer("")
    assert len(observers) == 2

    observable.remove_observer(None)
    assert len(observers) == 2


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
