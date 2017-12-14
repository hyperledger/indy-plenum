import pytest

from plenum.common.constants import PRE_REPLY_SENT, NODE_HOOKS
from plenum.common.hook_manager import HookManager
from plenum.common.messages.node_messages import Reply, ObservedData
from plenum.server.observer.observable import Observable
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType
from plenum.test.primary_selection.test_primary_selector import FakeNode
from plenum.test.testable import spyable


class TestObservable(Observable):
    def send_to_observers(self, msg: ObservedData, observer_remote_ids):
        pass


@spyable(methods=[TestObservable.send_to_observers])
class SpyableTestObservable(TestObservable):
    pass


class FakeReplyNode(FakeNode, HookManager):
    def __init__(self, tmpdir):
        FakeNode.__init__(self, tmpdir)
        HookManager.__init__(self, NODE_HOOKS)

    def send_reply(self):
        txn = {'some_key': 'some_value'}
        reply = Reply(txn)
        self.execute_hook(PRE_REPLY_SENT, reply=reply)


@pytest.fixture()
def node(tmpdir):
    return FakeReplyNode(tmpdir)


@pytest.fixture()
def unregistered_observable():
    return SpyableTestObservable()


@pytest.fixture()
def observable(unregistered_observable, node):
    unregistered_observable.register_observable(node)
    return unregistered_observable


@pytest.fixture()
def observers(observable):
    return observable.get_observers(ObserverSyncPolicyType.EACH_REPLY)


@pytest.fixture()
def policy_each_reply(observable):
    return observable._get_policy(ObserverSyncPolicyType.EACH_REPLY)
