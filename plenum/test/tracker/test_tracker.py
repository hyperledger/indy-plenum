import pytest
import random

from plenum.common.util import randomString
from plenum.common.tracker import Tracker


@pytest.fixture()
def state_root():
    return randomString(32).encode()


@pytest.fixture()
def make_tracker():
    return Tracker()


def test_error_with_no_state_root_track_uncommitted(make_tracker):
    with pytest.raises(AttributeError):
        make_tracker.track_uncommitted("", 12)


def test_error_with_invalid_ledger_size(make_tracker, state_root):
    with pytest.raises(AttributeError):
        make_tracker.track_uncommitted(state_root, random.randint(-99, 0))


def test_error_with_only_one_size_tuple(make_tracker):
    with pytest.raises(AttributeError):
        make_tracker.track_uncommitted(state_root, random.randint(1, 99))
