import pytest
import random

from plenum.common.util import randomString
from plenum.common.ledger_uncommitted_tracker import LedgerUncommittedTracker
from common.exceptions import PlenumValueError, LogicError


@pytest.fixture()
def state_root():
    return randomString(32).encode()


@pytest.fixture()
def init_committed_root():
    return "initial_committed_root_hash"

@pytest.fixture()
def init_ledger_size():
    return 10


@pytest.fixture()
def make_tracker(init_committed_root, init_ledger_size):
    return LedgerUncommittedTracker(init_committed_root, init_ledger_size)


def test_reject_batch_errors_when_no_uncommitted(make_tracker):
    with pytest.raises(LogicError):
        make_tracker.reject_batch()


def test_last_committed_is_equal_when_one_item_uncommitted(make_tracker,
                                                           init_committed_root,
                                                           state_root,
                                                           init_ledger_size):
    uncommitted_txns_count = 5
    make_tracker.apply_batch(state_root, init_ledger_size + uncommitted_txns_count)
    test_tuple = (init_committed_root, uncommitted_txns_count)
    assert test_tuple == make_tracker.reject_batch()


def test_last_committed_is_equal_when_multiple_items_uncommitted(make_tracker,
                                                                 init_committed_root):
    make_tracker.apply_batch("uncommitted_state_root_1", 12)
    make_tracker.apply_batch("uncommitted_state_root_2", 16)
    assert ("uncommitted_state_root_1", 4) == make_tracker.reject_batch()
    assert (init_committed_root, 2) == make_tracker.reject_batch()


def test_error_with_no_state_root_track_uncommitted(make_tracker):
    with pytest.raises(PlenumValueError):
        make_tracker.apply_batch("", 12)


def test_error_with_invalid_ledger_size(make_tracker, state_root):
    with pytest.raises(PlenumValueError):
        make_tracker.apply_batch(state_root, random.randint(-99, 0))


def test_error_with_revert_empty_tracker(make_tracker):
    with pytest.raises(LogicError):
        make_tracker.reject_batch()


def test_apply_batch_success(make_tracker, state_root):
    make_tracker.apply_batch(state_root, random.randint(1, 100))


def test_commit_batch_success(make_tracker,
                              state_root,
                              init_ledger_size):
    test_ledger_size = 1000
    test_root = "test_root"
    make_tracker.apply_batch(test_root, test_ledger_size)
    make_tracker.commit_batch()
    assert make_tracker.last_committed == (test_root, test_ledger_size)


def test_raise_error_if_commit_without_un_committed(make_tracker):
    with pytest.raises(PlenumValueError, match="commit_batch was called, but there is no tracked uncommitted states"):
        make_tracker.commit_batch()


def test_not_remove_last_committed_after_reject_last_batch(make_tracker):
    test_ledger_size = 1000
    test_root = "test_root"
    make_tracker.apply_batch(test_root, test_ledger_size)
    make_tracker.reject_batch()
    assert make_tracker.last_committed is not None


def test_apply_batch_with_zero_ledger_size(make_tracker):
    test_ledger_size = 0
    test_root = "test_root"
    make_tracker.apply_batch(test_root, test_ledger_size)


def test_set_last_committed(make_tracker):
    test_tuple = ('some_state_root', 42)
    assert make_tracker.last_committed != test_tuple
    make_tracker.set_last_committed(*test_tuple)
    assert make_tracker.last_committed == test_tuple
