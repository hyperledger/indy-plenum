import pytest
import random

from plenum.common.util import randomString
from plenum.common.ledger_uncommitted_tracker import LedgerUncommittedTracker
from common.exceptions import PlenumValueError, LogicError


@pytest.fixture()
def state_root():
    return randomString(32).encode()


@pytest.fixture()
def make_tracker():
    return LedgerUncommittedTracker()


def test_reject_batch_errors_when_no_uncommitted(make_tracker):
    with pytest.raises(LogicError):
        make_tracker.reject_batch()


def test_last_committed_is_equal_when_one_item_uncommitted(make_tracker, state_root):
    make_tracker.last_committed = ("this_is_a_fake_state_root", 10)
    last_committed_hash, last_committed_size = make_tracker.last_committed
    make_tracker.apply_batch(state_root, 5)
    _, last_uncommitted_size = make_tracker.un_committed[-1]
    test_tuple = (last_committed_hash, last_uncommitted_size - last_committed_size)
    assert test_tuple == make_tracker.reject_batch()


def test_last_committed_is_equal_when_multiple_items_uncommitted(make_tracker):
    make_tracker.last_committed = ("this_is_a_fake_state_root", 10)
    for i in range(10, 45, 5):
        make_tracker.apply_batch(randomString(32).encode(), i)
    _, pop_size = make_tracker.un_committed[-1]
    hash_after_pop, size_after_pop = make_tracker.un_committed[-2]
    test_tuple = (hash_after_pop, pop_size - size_after_pop)
    assert test_tuple == make_tracker.reject_batch()


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


def test_commit_batch_success(make_tracker, state_root):
    test_tuple = ("test_root", 1000)
    make_tracker.apply_batch(test_tuple[0], test_tuple[1])
    [make_tracker.apply_batch(state_root, i + 1) for i in range(1, 9)]
    assert make_tracker.commit_batch() == test_tuple


def test_reject_batch_success(make_tracker):
    test_tuple = ("test_root", 1000)
    [make_tracker.apply_batch(state_root, i + 1) for i in range(1, 9)]
    make_tracker.apply_batch("test_root", 1000)
    assert make_tracker.reject_batch() == test_tuple
