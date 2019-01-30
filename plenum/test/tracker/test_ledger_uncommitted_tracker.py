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
