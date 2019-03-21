import pytest
import random

from plenum.common.util import randomString
from plenum.common.ledger_uncommitted_tracker import LedgerUncommittedTracker
from common.exceptions import PlenumValueError, LogicError


@pytest.fixture()
def state_root():
    return randomString(32).encode()


@pytest.fixture()
def txn_root():
    return randomString(32).encode()


@pytest.fixture()
def init_committed_root():
    return "initial_committed_root_hash"


@pytest.fixture()
def init_txn_root():
    return "initial_committed_txn_root"


@pytest.fixture()
def init_ledger_size():
    return 10


@pytest.fixture()
def tracker(init_committed_root, init_txn_root, init_ledger_size):
    return LedgerUncommittedTracker(init_committed_root, init_txn_root, init_ledger_size)


@pytest.fixture()
def tracker_no_state(init_ledger_size):
    return LedgerUncommittedTracker(None, None, init_ledger_size)


def test_apply_batch_success(tracker, state_root, txn_root):
    tracker.apply_batch(state_root, txn_root, random.randint(1, 100))


def test_apply_batch_success_no_state(tracker):
    tracker.apply_batch(None, None, random.randint(1, 100))


def test_apply_no_state_root_track_uncommitted(tracker):
    tracker.apply_batch("", "", 12)
    tracker.apply_batch(None, None, 15)


def test_apply_batch_with_zero_ledger_size(tracker):
    tracker.apply_batch("test_root", "some_txn_root", 0)


def test_apply_error_with_invalid_ledger_size(tracker, state_root, txn_root):
    with pytest.raises(PlenumValueError):
        tracker.apply_batch(state_root, txn_root, random.randint(-99, 0))


def test_reject_one(tracker,
                    init_committed_root,
                    init_txn_root,
                    state_root,
                    txn_root,
                    init_ledger_size):
    tracker.apply_batch(state_root, txn_root, init_ledger_size + 5)
    assert (init_committed_root, init_txn_root, 5) == tracker.reject_batch()


def test_reject_one_no_state(tracker_no_state,
                             init_ledger_size):
    tracker_no_state.apply_batch(None, None, init_ledger_size + 5)
    assert (None, None, 5) == tracker_no_state.reject_batch()


def test_reject_multiple(tracker,
                         init_committed_root,
                         init_txn_root):
    tracker.apply_batch("uncommitted_state_root_1", "uncommitted_txn_root_1", 12)
    tracker.apply_batch("uncommitted_state_root_2", "uncommitted_txn_root_2", 16)
    assert ("uncommitted_state_root_1", "uncommitted_txn_root_1", 4) == tracker.reject_batch()
    assert (init_committed_root, init_txn_root, 2) == tracker.reject_batch()


def test_reject_error_with_empty_tracker(tracker):
    with pytest.raises(LogicError):
        tracker.reject_batch()


def test_commit_one_batch(tracker,
                          init_ledger_size):
    tracker.apply_batch("test_root", "test_txn_root", 1000)
    assert tracker.commit_batch() == ("test_root", "test_txn_root", 1000 - init_ledger_size)
    assert tracker.last_committed == ("test_root", "test_txn_root", 1000)


def test_commit_one_batch_no_state(tracker_no_state,
                                   init_ledger_size):
    tracker_no_state.apply_batch(None, None, 1000)
    assert tracker_no_state.commit_batch() == (None, None, 1000 - init_ledger_size)
    assert tracker_no_state.last_committed == (None, None, 1000)


def test_commit_multiple_batches(tracker,
                                 init_ledger_size):
    tracker.apply_batch("test_root_1", "test_txn_root_1", 1000)
    tracker.apply_batch("test_root_2", "test_txn_root_2", 1500)
    tracker.apply_batch("test_root_3", "test_txn_root_3", 1700)

    assert tracker.commit_batch() == ("test_root_1", "test_txn_root_1", 1000 - init_ledger_size)
    assert tracker.last_committed == ("test_root_1", "test_txn_root_1", 1000)

    assert tracker.commit_batch() == ("test_root_2", "test_txn_root_2", 500)
    assert tracker.last_committed == ("test_root_2", "test_txn_root_2", 1500)

    assert tracker.commit_batch() == ("test_root_3", "test_txn_root_3", 200)
    assert tracker.last_committed == ("test_root_3", "test_txn_root_3", 1700)


def test_apply_reject_commit(tracker,
                             init_ledger_size):
    tracker.apply_batch("test_root_1", "test_txn_root_1", 1000)
    tracker.apply_batch("test_root_2", "test_txn_root_2", 1600)

    assert ("test_root_1", "test_txn_root_1", 600) == tracker.reject_batch()

    assert tracker.commit_batch() == ("test_root_1", "test_txn_root_1", 1000 - init_ledger_size)
    assert tracker.last_committed == ("test_root_1", "test_txn_root_1", 1000)


def test_revert_all_and_apply_commit(tracker,
                                     init_committed_root,
                                     init_txn_root,
                                     init_ledger_size):
    tracker.apply_batch("test_root_1", "test_txn_root_1", init_ledger_size + 1000)
    tracker.apply_batch("test_root_2", "test_txn_root_2", init_ledger_size + 1600)

    assert ("test_root_1", "test_txn_root_1", 600) == tracker.reject_batch()
    assert (init_committed_root, init_txn_root, 1000) == tracker.reject_batch()

    tracker.apply_batch("test_root_3", "test_txn_root_3", 100)
    tracker.apply_batch("test_root_4", "test_txn_root_4", 300)

    assert tracker.commit_batch() == ("test_root_3", "test_txn_root_3", 100 - init_ledger_size)
    assert tracker.last_committed == ("test_root_3", "test_txn_root_3", 100)

    assert tracker.commit_batch() == ("test_root_4", "test_txn_root_4", 200)
    assert tracker.last_committed == ("test_root_4", "test_txn_root_4", 300)


def test_apply_reject_apply_commit(tracker,
                                   init_ledger_size):
    tracker.apply_batch("test_root_1", "test_txn_root_1", 1000)
    tracker.apply_batch("test_root_2", "test_txn_root_2", 1600)

    assert ("test_root_1", "test_txn_root_1", 600) == tracker.reject_batch()

    tracker.apply_batch("test_root_3", "test_txn_root_3", 1200)

    assert tracker.commit_batch() == ("test_root_1", "test_txn_root_1", 1000 - init_ledger_size)
    assert tracker.last_committed == ("test_root_1", "test_txn_root_1", 1000)

    assert tracker.commit_batch() == ("test_root_3", "test_txn_root_3", 200)
    assert tracker.last_committed == ("test_root_3", "test_txn_root_3", 1200)


def test_commit_apply(tracker,
                      init_ledger_size):
    tracker.apply_batch("test_root_1", "test_txn_root_1", 1000)
    assert tracker.commit_batch() == ("test_root_1", "test_txn_root_1", 1000 - init_ledger_size)
    assert tracker.last_committed == ("test_root_1", "test_txn_root_1", 1000)

    tracker.apply_batch("test_root_2", "test_txn_root_2", 1400)
    assert tracker.commit_batch() == ("test_root_2", "test_txn_root_2", 400)
    assert tracker.last_committed == ("test_root_2", "test_txn_root_2", 1400)

    tracker.apply_batch("test_root_3", "test_txn_root_3", 1700)
    assert tracker.commit_batch() == ("test_root_3", "test_txn_root_3", 300)
    assert tracker.last_committed == ("test_root_3", "test_txn_root_3", 1700)


def test_commit_raise_error_if_commit_without_un_committed(tracker):
    with pytest.raises(PlenumValueError, match="commit_batch was called, but there is no tracked uncommitted states"):
        tracker.commit_batch()


def test_not_remove_last_committed_after_reject_last_batch(tracker,
                                                           init_committed_root,
                                                           init_txn_root,
                                                           init_ledger_size):
    tracker.apply_batch("test_root", "test_txn_root", 1000)
    tracker.reject_batch()
    assert tracker.last_committed == (init_committed_root, init_txn_root, init_ledger_size)


def test_set_last_committed(tracker):
    test_tuple = ('some_state_root', 'some_txn_root', 42)
    assert tracker.last_committed != test_tuple
    tracker.set_last_committed(*test_tuple)
    assert tracker.last_committed == test_tuple


def test_set_last_committed_no_state(tracker_no_state):
    test_tuple = (None, None, 42)
    assert tracker_no_state.last_committed != test_tuple
    tracker_no_state.set_last_committed(*test_tuple)
    assert tracker_no_state.last_committed == test_tuple


def test_apply_commit_after_set_lst_comitted(tracker,
                                             init_ledger_size):
    tracker.apply_batch("test_root_1", "test_txn_root_1", 1000)
    assert tracker.commit_batch() == ("test_root_1", "test_txn_root_1", 1000 - init_ledger_size)
    assert tracker.last_committed == ("test_root_1", "test_txn_root_1", 1000)

    tracker.set_last_committed("test_root_2", "test_txn_root_2", 2000)
    assert tracker.last_committed == ("test_root_2", "test_txn_root_2", 2000)

    tracker.apply_batch("test_root_3", "test_txn_root_3", 2200)
    tracker.apply_batch("test_root_4", "test_txn_root_4", 2500)

    assert tracker.commit_batch() == ("test_root_3", "test_txn_root_3", 200)
    assert tracker.last_committed == ("test_root_3", "test_txn_root_3", 2200)

    assert tracker.commit_batch() == ("test_root_4", "test_txn_root_4", 300)
    assert tracker.last_committed == ("test_root_4", "test_txn_root_4", 2500)

    tracker.apply_batch("test_root_5", "test_txn_root_5", 3000)
    assert tracker.commit_batch() == ("test_root_5", "test_txn_root_5", 500)
    assert tracker.last_committed == ("test_root_5", "test_txn_root_5", 3000)
