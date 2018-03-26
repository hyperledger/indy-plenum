import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, CURRENT_PROTOCOL_VERSION
from plenum.common.messages.node_messages import BatchCommitted
from plenum.common.util import get_utc_epoch
from plenum.server.observer.observable import Observable
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType
from plenum.test.bls.helper import generate_state_root
from plenum.test.helper import sdk_random_request_objects
from plenum.test.test_node import TestNode
from plenum.test.testable import spyable


@spyable(methods=[Observable.append_input,
                  Observable.send_to_observers,
                  Observable.process_new_batch,
                  ])
class TestObservable(Observable):
    pass


class TestNodeWithObservable(TestNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._observable = TestObservable()

    def _service_observable_out_box(self, limit: int = None) -> int:
        return 0


@pytest.fixture(scope="module")
def testNodeClass(patchPluginManager):
    return TestNodeWithObservable


@pytest.fixture(scope="module")
def node(txnPoolNodeSet):
    return txnPoolNodeSet[0]


@pytest.fixture(scope="module")
def node_observable(node):
    return node._observable


@pytest.fixture()
def observable():
    return TestObservable()


@pytest.fixture()
def observers(observable):
    return observable.get_observers(ObserverSyncPolicyType.EACH_BATCH)


@pytest.fixture()
def policy_each_reply(observable):
    return observable._get_policy(ObserverSyncPolicyType.EACH_BATCH)


@pytest.fixture()
def fake_msg_batch_committed():
    reqs = [req.as_dict for req in
            sdk_random_request_objects(10, identifier="1" * 16, protocol_version=CURRENT_PROTOCOL_VERSION)]
    return BatchCommitted(reqs,
                          DOMAIN_LEDGER_ID,
                          get_utc_epoch(),
                          generate_state_root(),
                          generate_state_root(),
                          1,
                          10)
