import pytest

from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType


def test_register_observable(observable):
    assert observable
    assert observable._get_policy(ObserverSyncPolicyType.EACH_REPLY)


def test_add_observer_to_unregistered(unregistered_observable):
    with pytest.raises(AssertionError) as ex:
        unregistered_observable.add_observer("observer1", ObserverSyncPolicyType.EACH_REPLY)
        assert "call `register_observable` first" == str(ex.value)


def test_remove_observer_from_unregistered(unregistered_observable):
    with pytest.raises(AssertionError) as ex:
        unregistered_observable.remove_observer("observer1")
        assert "call `register_observable` first" == str(ex.value)


def test_add_valid_observer(observable, observers):
    assert len(observers) == 0

    observable.add_observer("observer1", ObserverSyncPolicyType.EACH_REPLY)
    assert len(observers) == 1
    assert observers[0] == "observer1"

    observable.add_observer("observer2", ObserverSyncPolicyType.EACH_REPLY)
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
    observable.add_observer("observer1", ObserverSyncPolicyType.EACH_REPLY)
    observable.add_observer("observer2", ObserverSyncPolicyType.EACH_REPLY)
    assert len(observers) == 2

    observable.remove_observer("observer1")
    assert len(observers) == 1
    assert observers[0] == "observer2"

    observable.remove_observer("observer2")
    assert len(observers) == 0


def test_remove_invalid_observer(observable, observers):
    observable.add_observer("observer1", ObserverSyncPolicyType.EACH_REPLY)
    observable.add_observer("observer2", ObserverSyncPolicyType.EACH_REPLY)
    assert len(observers) == 2

    observable.remove_observer("observer3")
    assert len(observers) == 2

    observable.remove_observer("")
    assert len(observers) == 2

    observable.remove_observer(None)
    assert len(observers) == 2
