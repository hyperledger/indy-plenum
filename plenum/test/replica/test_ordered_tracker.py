from random import randint

import pytest
from plenum.server.replica import IntervalList, OrderedTracker


@pytest.fixture
def intervals():
    return IntervalList()


@pytest.fixture
def other_intervals():
    return IntervalList()


@pytest.fixture
def tracker():
    return OrderedTracker()


@pytest.fixture
def other_tracker():
    return OrderedTracker()


def test_interval_list_has_contains_accessor(intervals: IntervalList):
    assert len(intervals) == 0
    assert 1 not in intervals


def test_interval_list_can_add_value(intervals: IntervalList):
    intervals.add(1)

    assert len(intervals) == 1
    assert 0 not in intervals
    assert 1 in intervals
    assert 2 not in intervals


def test_interval_list_can_add_continuous_blocks(intervals: IntervalList):
    intervals.add(1)
    intervals.add(2)

    assert len(intervals) == 2
    assert 0 not in intervals
    assert 1 in intervals
    assert 2 in intervals
    assert 3 not in intervals


def test_interval_list_can_grow_blocks_in_arbitrary_direction(intervals: IntervalList):
    intervals.add(1)
    intervals.add(2)
    intervals.add(0)

    assert len(intervals) == 3
    assert -1 not in intervals
    assert 0 in intervals
    assert 1 in intervals
    assert 2 in intervals
    assert 3 not in intervals


def test_interval_list_can_have_multiple_blocks(intervals: IntervalList):
    intervals.add(4)
    intervals.add(5)
    intervals.add(1)
    intervals.add(2)

    assert len(intervals) == 4
    assert 0 not in intervals
    assert 1 in intervals
    assert 2 in intervals
    assert 3 not in intervals
    assert 4 in intervals
    assert 5 in intervals
    assert 6 not in intervals


def test_interval_list_can_handles_duplicate_entries_correctly(intervals: IntervalList):
    intervals.add(1)
    intervals.add(1)

    assert len(intervals) == 1
    assert 0 not in intervals
    assert 1 in intervals
    assert 2 not in intervals


def test_intervals_dont_overlap(intervals: IntervalList):
    intervals.add(1)
    intervals.add(2)
    intervals.add(4)
    intervals.add(5)
    intervals.add(3)
    intervals.add(4)

    assert len(intervals) == 5


def test_interval_list_has_equality_operator(intervals: IntervalList, other_intervals: IntervalList):
    assert intervals == other_intervals

    intervals.add(1)
    assert intervals != other_intervals

    other_intervals.add(1)
    assert intervals == other_intervals


def test_interval_list_handles_arbitrary_insertion_order(intervals: IntervalList, other_intervals: IntervalList):
    test_set = set()
    for _ in range(100):
        n = randint(1,100)
        test_set.add(n)
        intervals.add(n)

    for n in test_set:
        other_intervals.add(n)

    assert len(intervals) == len(test_set)
    assert intervals == other_intervals
    for _ in range(200):
        n = randint(0,200)
        if n in test_set:
            assert n in intervals
        else:
            assert n not in intervals


def test_ordered_tracker_has_contains_accessor(tracker: OrderedTracker):
    assert len(tracker) == 0
    assert not (0, 1) in tracker


def test_ordered_tracker_can_register_batches(tracker: OrderedTracker):
    tracker.add(0, 1)
    assert len(tracker) == 1
    assert (0, 1) in tracker


def test_ordered_tracker_has_equality_operator(tracker: OrderedTracker, other_tracker: OrderedTracker):
    assert tracker == other_tracker

    tracker.add(0, 1)
    assert tracker != other_tracker

    other_tracker.add(1, 3)
    assert tracker != other_tracker

    tracker.add(1, 3)
    other_tracker.add(0, 1)
    assert tracker == other_tracker


def test_ordered_tracker_behaves_like_a_set(tracker: OrderedTracker):
    test_set = set()
    for _ in range(1000):
        v = randint(0,10)
        n = randint(1,100)
        test_set.add((v, n))
        tracker.add(v, n)

    assert len(tracker) == len(test_set)
    for _ in range(2000):
        v = randint(0,20)
        n = randint(0,200)
        if (v, n) in test_set:
            assert (v, n) in tracker
        else:
            assert (v, n) not in tracker


def test_ordered_tracker_can_clean_old_views(tracker: OrderedTracker):
    test_set = set()
    for _ in range(1000):
        v = randint(0,10)
        n = randint(1,100)
        if v >= 5:
            test_set.add((v, n))
        tracker.add(v, n)

    tracker.clear_below_view(5)

    assert len(tracker) == len(test_set)
    for _ in range(2000):
        v = randint(0,20)
        n = randint(0,200)
        if (v, n) in test_set:
            assert (v, n) in tracker
        else:
            assert (v, n) not in tracker
