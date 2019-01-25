import pytest
import random

from plenum.common.util import randomString
from plenum.common.tracker import Tracker
from common.exceptions import PlenumValueError, LogicError


@pytest.fixture()
def state_root():
    return randomString(32).encode()


@pytest.fixture()
def make_tracker():
    return Tracker()


def test_error_with_no_state_root_track_uncommitted(make_tracker):
    with pytest.raises(PlenumValueError):
        make_tracker.apply_batch("", 12)


def test_error_with_invalid_ledger_size(make_tracker, state_root):
    with pytest.raises(PlenumValueError):
        make_tracker.apply_batch(state_root, random.randint(-99, 0))


def test_error_with_revert_empty_tracker(make_tracker):
    with pytest.raises(LogicError):
        make_tracker.reject_batch()
