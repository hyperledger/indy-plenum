from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType
from plenum.test.spy_helpers import get_count


def test_can_process(policy_each_reply):
    assert policy_each_reply.can_process(ObserverSyncPolicyType.EACH_REPLY)
    assert not policy_each_reply.can_process("Unknown")
    assert not policy_each_reply.can_process("")
    assert not policy_each_reply.can_process(None)


def test_observes_each_reply(observable, node):
    assert 0 == get_count(observable, observable.send_to_observers)
    node.send_reply()
    assert 1 == get_count(observable, observable.send_to_observers)
