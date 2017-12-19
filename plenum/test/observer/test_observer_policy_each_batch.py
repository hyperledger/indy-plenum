import types

import pytest

from plenum.common.constants import BATCH, DOMAIN_LEDGER_ID, CURRENT_PROTOCOL_VERSION
from plenum.common.messages.node_messages import BatchCommitted, ObservedData
from plenum.common.util import get_utc_epoch
from plenum.server.observer.observer_sync_policy_each_batch import ObserverSyncPolicyEachBatch
from plenum.test.bls.helper import generate_state_root
from plenum.test.helper import sdk_random_request_objects


def create_observed_data(ts=None, req_num=1):
    reqs = tuple([req.as_dict for req in sdk_random_request_objects(
        req_num, identifier="1" * 16, protocol_version=CURRENT_PROTOCOL_VERSION)])
    ts = ts or get_utc_epoch()
    msg = BatchCommitted(reqs,
                         DOMAIN_LEDGER_ID,
                         ts,
                         generate_state_root(),
                         generate_state_root())
    return ObservedData(BATCH, msg)


@pytest.fixture()
def observed_data_msg():
    return create_observed_data()


@pytest.fixture()
def observer_policy(node):
    def patched_do_apply_batch(self, batch):
        self.applied_num += 1

    policy = ObserverSyncPolicyEachBatch(node)
    policy.applied_num = 0
    policy._do_apply_batch = types.MethodType(patched_do_apply_batch, policy)

    return policy


def test_policy_type(observer_policy):
    assert observer_policy.policy_type == BATCH


def test_can_process_batch(observer_policy):
    assert observer_policy._can_process(create_observed_data())

    observer_policy._last_applied_ts = 1599906902
    assert not observer_policy._can_process(create_observed_data(ts=1599906902))
    assert not observer_policy._can_process(create_observed_data(ts=1599906901))
    assert observer_policy._can_process(create_observed_data(ts=1599906903))
    assert observer_policy._can_process(create_observed_data(ts=1599906904))


def test_apply_one_batch_quorum(observer_policy, observed_data_msg):
    observer_policy.apply_data(observed_data_msg, "Node1")
    assert observer_policy.applied_num == 0

    observer_policy.apply_data(observed_data_msg, "Node2")
    assert observer_policy.applied_num == 1


def test_apply_one_batch_same_sender(observer_policy, observed_data_msg):
    observer_policy.apply_data(observed_data_msg, "Node1")
    assert observer_policy.applied_num == 0

    observer_policy.apply_data(observed_data_msg, "Node1")
    assert observer_policy.applied_num == 0

    observer_policy.apply_data(observed_data_msg, "Node1")
    assert observer_policy.applied_num == 0


def test_apply_processed_batch(observer_policy, observed_data_msg):
    observer_policy.apply_data(observed_data_msg, "Node1")
    observer_policy.apply_data(observed_data_msg, "Node2")
    assert observer_policy.applied_num == 1

    observer_policy.apply_data(observed_data_msg, "Node1")
    assert observer_policy.applied_num == 1
    observer_policy.apply_data(observed_data_msg, "Node2")
    assert observer_policy.applied_num == 1
    observer_policy.apply_data(observed_data_msg, "Node3")
    assert observer_policy.applied_num == 1


def test_apply_multiple_batches_sequentially(observer_policy):
    msg1 = create_observed_data(ts=1599906903)
    observer_policy.apply_data(msg1, "Node1")
    observer_policy.apply_data(msg1, "Node2")
    assert observer_policy.applied_num == 1

    msg2 = create_observed_data(ts=1599906904)
    observer_policy.apply_data(msg2, "Node1")
    observer_policy.apply_data(msg2, "Node2")
    assert observer_policy.applied_num == 2

    msg3 = create_observed_data(ts=1599906905)
    msg4 = create_observed_data(ts=1599906906)
    observer_policy.apply_data(msg3, "Node1")
    observer_policy.apply_data(msg4, "Node1")
    assert observer_policy.applied_num == 2

    observer_policy.apply_data(msg3, "Node2")
    assert observer_policy.applied_num == 3

    observer_policy.apply_data(msg4, "Node2")
    assert observer_policy.applied_num == 4


def test_apply_multiple_batches_in_different_order(observer_policy):
    msg1 = create_observed_data(ts=1599906903)
    msg2 = create_observed_data(ts=1599906904)
    msg3 = create_observed_data(ts=1599906905)

    observer_policy.apply_data(msg3, "Node1")
    observer_policy.apply_data(msg2, "Node1")
    observer_policy.apply_data(msg1, "Node1")
    assert observer_policy.applied_num == 0

    observer_policy.apply_data(msg3, "Node2")
    assert observer_policy.applied_num == 0

    observer_policy.apply_data(msg2, "Node2")
    assert observer_policy.applied_num == 0

    observer_policy.apply_data(msg1, "Node2")
    assert observer_policy.applied_num == 3
