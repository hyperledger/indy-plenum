from random import randint
from typing import List

import pytest

from plenum.common.stashing_router import UnsortedStash, SortedStash


@pytest.fixture(params=[10, 20])
def queue_size(request):
    return request.param


@pytest.fixture
def unsorted_stash(queue_size):
    return UnsortedStash(queue_size)


@pytest.fixture(params=["normal", "reversed"])
def sort_key(request):
    if request.param == "normal":
        return lambda v: v
    if request.param == "reversed":
        return lambda v: -v


@pytest.fixture
def sorted_stash(queue_size, sort_key):
    return SortedStash(queue_size, key=sort_key)


@pytest.fixture(params=["unsorted", "sorted"])
def stashing_queue(request, unsorted_stash, sorted_stash):
    if request.param == "unsorted":
        return unsorted_stash
    if request.param == "sorted":
        return sorted_stash


def some_item():
    return randint(0, 100)


def tuplify(data):
    if isinstance(data, List):
        return [(item,) for item in data]
    else:
        return (data,)


def test_stashing_queue_is_initially_empty(stashing_queue):
    assert not stashing_queue
    assert len(stashing_queue) == 0


def test_adding_item_to_stashing_queue_makes_it_non_empty(stashing_queue):
    assert stashing_queue.push(some_item())
    assert stashing_queue
    assert len(stashing_queue) == 1


def test_multiple_items_can_be_added_to_stashing_queue(stashing_queue):
    for _ in range(3):
        assert stashing_queue.push(some_item())
    assert stashing_queue
    assert len(stashing_queue) == 3


def test_pop_from_empty_queue_raises_index_error(stashing_queue):
    with pytest.raises(IndexError):
        stashing_queue.pop()


def test_pop_all_from_empty_queue_returns_empty_list(stashing_queue):
    assert stashing_queue.pop_all() == []


def test_unsorted_stash_pops_items_in_original_order(unsorted_stash):
    items = [some_item() for _ in range(7)]
    for item in items:
        unsorted_stash.push(item)

    expected_len = len(items)
    for i in range(len(items)):
        item = unsorted_stash.pop()
        assert item == tuplify(items[i])

        expected_len -= 1
        assert len(unsorted_stash) == expected_len


def test_unsorted_stash_pops_all_items_in_original_order(unsorted_stash):
    items = [some_item() for _ in range(7)]
    for item in items:
        unsorted_stash.push(item)

    popped_items = unsorted_stash.pop_all()
    assert popped_items == tuplify(items)
    assert len(unsorted_stash) == 0


def test_sorted_stash_pops_items_in_sort_order(sorted_stash, sort_key):
    items = [some_item() for _ in range(7)]
    for item in items:
        sorted_stash.push(item)

    sorted_items = sorted(items, key=sort_key)
    expected_len = len(items)
    for i in range(len(items)):
        item = sorted_stash.pop()
        assert item == tuplify(sorted_items[i])

        expected_len -= 1
        assert len(sorted_stash) == expected_len


def test_sorted_stash_pops_all_items_in_sort_order(sorted_stash, sort_key):
    items = [some_item() for _ in range(7)]
    for item in items:
        sorted_stash.push(item)

    popped_items = sorted_stash.pop_all()
    assert popped_items == tuplify(sorted(items, key=sort_key))
    assert len(sorted_stash) == 0


def test_up_to_queue_size_items_can_be_added_to_stashing_queue(stashing_queue, queue_size):
    items = [some_item() for _ in range(queue_size)]

    for item in items:
        assert stashing_queue.push(item)
    assert len(stashing_queue) == len(items)

    for _ in range(3):
        assert not stashing_queue.push(some_item())
        assert len(stashing_queue) == queue_size

    assert tuplify(sorted(items)) == sorted(stashing_queue.pop_all())
