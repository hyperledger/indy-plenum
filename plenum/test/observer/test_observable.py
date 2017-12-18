from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType


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
