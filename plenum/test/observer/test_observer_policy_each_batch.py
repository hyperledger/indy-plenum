import types

import pytest

from common.exceptions import PlenumValueError
from plenum.common.constants import BATCH, DOMAIN_LEDGER_ID, CURRENT_PROTOCOL_VERSION
from plenum.common.messages.node_messages import BatchCommitted, ObservedData
from plenum.common.util import get_utc_epoch
from plenum.server.observer.observer_sync_policy_each_batch import ObserverSyncPolicyEachBatch
from plenum.test.helper import sdk_random_request_objects, generate_state_root


def create_observed_data(seq_no_start=1, seq_no_end=5):
    req_num = seq_no_end - seq_no_start + 1
    reqs = [req.as_dict for req in sdk_random_request_objects(
        req_num, identifier="1" * 16, protocol_version=CURRENT_PROTOCOL_VERSION)]
    msg = BatchCommitted(reqs,
                         DOMAIN_LEDGER_ID,
                         0,
                         1,
                         1,
                         get_utc_epoch(),
                         generate_state_root(),
                         generate_state_root(),
                         seq_no_start,
                         seq_no_end,
                         generate_state_root(),
                         ['Alpha', 'Beta'],
                         ['Alpha', 'Beta', 'Gamma', 'Delta'],
                         0,
                         'digest')
    return ObservedData(BATCH, msg)


@pytest.fixture()
def observed_data_msg():
    return create_observed_data()


@pytest.fixture()
def observed_data_transferred(node):
    '''
    Emulate the message as how it comes from other Nodes
    '''
    msg = node.nodestack.deserializeMsg(
        node.nodestack.sign_and_serialize(
            create_observed_data())
    )
    return ObservedData(**msg)


@pytest.fixture()
def observer_policy(node):
    def patched_do_apply_batch(self, batch):
        self.applied_num += 1

    policy = ObserverSyncPolicyEachBatch(node)
    policy.applied_num = 0
    policy._do_apply_batch = types.MethodType(patched_do_apply_batch, policy)

    return policy


'''
In all the tests we assume that consensus quorum to apply ObservedData is f+1=2
'''


def test_policy_type(observer_policy):
    assert observer_policy.policy_type == BATCH


def test_apply_data_fails_on_invalid_args(observer_policy, observed_data_msg):
    observed_data_msg.msg_type = "NOT_BATCH"
    with pytest.raises(PlenumValueError) as excinfo:
        observer_policy.apply_data(observed_data_msg, "Node1")
    assert "expected: {}".format(BATCH) in str(excinfo.value)


def test_quorum_same_message(observer_policy, observed_data_msg):
    observer_policy.apply_data(observed_data_msg, "Node1")
    assert observer_policy.applied_num == 0

    observer_policy.apply_data(observed_data_msg, "Node2")
    assert observer_policy.applied_num == 1


def test_message_as_transferred(observer_policy, observed_data_transferred):
    observer_policy.apply_data(observed_data_transferred, "Node1")
    assert observer_policy.applied_num == 0

    observer_policy.apply_data(observed_data_transferred, "Node2")
    assert observer_policy.applied_num == 1


def test_quorum_diff_messages(observer_policy):
    msg1 = create_observed_data()
    msg2 = create_observed_data()
    msg3 = create_observed_data()

    observer_policy.apply_data(msg1, "Node1")
    assert observer_policy.applied_num == 0
    observer_policy.apply_data(msg2, "Node2")
    assert observer_policy.applied_num == 0
    observer_policy.apply_data(msg3, "Node3")
    assert observer_policy.applied_num == 0
    observer_policy.apply_data(msg1, "Node4")
    assert observer_policy.applied_num == 1


def test_no_quorum_diff_messages(observer_policy):
    msg1 = create_observed_data()
    msg2 = create_observed_data()
    msg3 = create_observed_data()
    msg4 = create_observed_data()

    observer_policy.apply_data(msg1, "Node1")
    assert observer_policy.applied_num == 0
    observer_policy.apply_data(msg2, "Node2")
    assert observer_policy.applied_num == 0
    observer_policy.apply_data(msg3, "Node3")
    assert observer_policy.applied_num == 0
    observer_policy.apply_data(msg4, "Node4")
    assert observer_policy.applied_num == 0


def test_apply_one_batch_same_sender(observer_policy, observed_data_msg):
    observer_policy.apply_data(observed_data_msg, "Node1")
    assert observer_policy.applied_num == 0

    observer_policy.apply_data(observed_data_msg, "Node1")
    assert observer_policy.applied_num == 0

    observer_policy.apply_data(observed_data_msg, "Node1")
    assert observer_policy.applied_num == 0

    observer_policy.apply_data(observed_data_msg, "Node2")
    assert observer_policy.applied_num == 1


def test_apply_already_processed_batch(observer_policy, observed_data_msg):
    observer_policy.apply_data(observed_data_msg, "Node1")
    observer_policy.apply_data(observed_data_msg, "Node2")
    assert observer_policy.applied_num == 1

    observer_policy.apply_data(observed_data_msg, "Node1")
    assert observer_policy.applied_num == 1
    observer_policy.apply_data(observed_data_msg, "Node2")
    assert observer_policy.applied_num == 1
    observer_policy.apply_data(observed_data_msg, "Node3")
    assert observer_policy.applied_num == 1
    observer_policy.apply_data(observed_data_msg, "Node4")
    assert observer_policy.applied_num == 1


def test_apply_multiple_batches_sequentially(observer_policy):
    msg1 = create_observed_data(seq_no_start=1, seq_no_end=1)
    observer_policy.apply_data(msg1, "Node1")
    observer_policy.apply_data(msg1, "Node2")
    assert observer_policy.applied_num == 1

    msg2 = create_observed_data(seq_no_start=2, seq_no_end=5)
    observer_policy.apply_data(msg2, "Node1")
    observer_policy.apply_data(msg2, "Node2")
    assert observer_policy.applied_num == 2

    msg3 = create_observed_data(seq_no_start=6, seq_no_end=26)
    msg4 = create_observed_data(seq_no_start=27, seq_no_end=27)
    observer_policy.apply_data(msg3, "Node1")
    observer_policy.apply_data(msg4, "Node1")
    assert observer_policy.applied_num == 2

    observer_policy.apply_data(msg3, "Node2")
    assert observer_policy.applied_num == 3

    observer_policy.apply_data(msg4, "Node2")
    assert observer_policy.applied_num == 4


def test_apply_multiple_batches_in_reverse_order(observer_policy):
    msg1 = create_observed_data(seq_no_start=1, seq_no_end=2)
    msg2 = create_observed_data(seq_no_start=3, seq_no_end=3)
    msg3 = create_observed_data(seq_no_start=4, seq_no_end=8)

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


def test_apply_multiple_batches_stashed(observer_policy):
    msg1 = create_observed_data(seq_no_start=1, seq_no_end=2)
    msg2 = create_observed_data(seq_no_start=3, seq_no_end=4)
    msg3 = create_observed_data(seq_no_start=5, seq_no_end=6)
    msg4 = create_observed_data(seq_no_start=7, seq_no_end=8)

    observer_policy.apply_data(msg1, "Node1")
    observer_policy.apply_data(msg2, "Node1")
    observer_policy.apply_data(msg3, "Node1")
    assert observer_policy.applied_num == 0

    observer_policy.apply_data(msg1, "Node2")
    assert observer_policy.applied_num == 1

    observer_policy.apply_data(msg3, "Node2")
    assert observer_policy.applied_num == 1

    observer_policy.apply_data(msg4, "Node1")
    assert observer_policy.applied_num == 1

    observer_policy.apply_data(msg2, "Node2")
    assert observer_policy.applied_num == 3

    observer_policy.apply_data(msg4, "Node2")
    assert observer_policy.applied_num == 4


def test_next_batch_comes_before_expected_one(observer_policy):
    msg1 = create_observed_data(seq_no_start=1, seq_no_end=2)
    msg2 = create_observed_data(seq_no_start=6, seq_no_end=7)
    msg3 = create_observed_data(seq_no_start=3, seq_no_end=5)

    observer_policy.apply_data(msg1, "Node1")
    observer_policy.apply_data(msg1, "Node2")
    assert observer_policy.applied_num == 1

    observer_policy.apply_data(msg2, "Node1")
    observer_policy.apply_data(msg2, "Node2")
    assert observer_policy.applied_num == 1

    observer_policy.apply_data(msg3, "Node1")
    observer_policy.apply_data(msg3, "Node2")
    assert observer_policy.applied_num == 3


def test_apply_multiple_batches_stashed_with_different_msgs(observer_policy):
    msg11 = create_observed_data(seq_no_start=1, seq_no_end=2)
    msg12 = create_observed_data(seq_no_start=1, seq_no_end=2)
    msg13 = create_observed_data(seq_no_start=1, seq_no_end=2)

    msg21 = create_observed_data(seq_no_start=6, seq_no_end=7)
    msg22 = create_observed_data(seq_no_start=6, seq_no_end=7)
    msg23 = create_observed_data(seq_no_start=6, seq_no_end=7)

    msg31 = create_observed_data(seq_no_start=3, seq_no_end=5)
    msg32 = create_observed_data(seq_no_start=3, seq_no_end=5)
    msg33 = create_observed_data(seq_no_start=3, seq_no_end=5)

    observer_policy.apply_data(msg11, "Node1")
    observer_policy.apply_data(msg12, "Node2")
    observer_policy.apply_data(msg13, "Node3")
    assert observer_policy.applied_num == 0
    observer_policy.apply_data(msg13, "Node4")
    assert observer_policy.applied_num == 1

    observer_policy.apply_data(msg21, "Node1")
    observer_policy.apply_data(msg22, "Node2")
    observer_policy.apply_data(msg23, "Node3")
    assert observer_policy.applied_num == 1
    observer_policy.apply_data(msg23, "Node4")
    assert observer_policy.applied_num == 1

    observer_policy.apply_data(msg31, "Node1")
    observer_policy.apply_data(msg32, "Node2")
    observer_policy.apply_data(msg33, "Node3")
    assert observer_policy.applied_num == 1
    observer_policy.apply_data(msg33, "Node4")
    assert observer_policy.applied_num == 3
