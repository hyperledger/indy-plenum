import math
from itertools import combinations

import time

import os
from libnacl import crypto_hash_sha256

from plenum.common.util import randomString, compare_3PC_keys, \
    check_if_all_equal_in_list, min_3PC_key, max_3PC_key, get_utc_epoch, \
    mostCommonElement
from stp_core.network.util import evenCompare, distributedConnectionMap
from plenum.test.greek import genNodeNames


def test_even_compare():
    vals = genNodeNames(24)
    for v1 in vals:
        beats = [v2 for v2 in vals if v1 != v2 and evenCompare(v1, v2)]
        print("{} beats {} others: {}".format(v1, len(beats), beats))
    evenCompare('Zeta', 'Alpha')

    def hashit(s):
        b = s.encode('utf-8')
        c = crypto_hash_sha256(b)
        return c.hex()

    for v in vals:
        print("{}: {}".format(v, hashit(v)))


def test_distributedConnectionMap():
    for nodeCount in range(2, 25):
        print("testing for node count: {}".format(nodeCount))
        names = genNodeNames(nodeCount)
        conmap = distributedConnectionMap(names)

        total_combinations = len(list(combinations(names, 2)))
        total_combinations_in_map = sum(len(x) for x in conmap.values())
        assert total_combinations_in_map == total_combinations

        maxPer = math.ceil(total_combinations / nodeCount)
        minPer = math.floor(total_combinations / nodeCount)
        for x in conmap.values():
            assert len(x) <= maxPer
            assert len(x) >= minPer


def test_distributedConnectionMapIsDeterministic():
    """
    While this doesn't prove determinism, it gives us confidence.
    For 20 iterations, it generates 24 random names, and 10 conmaps for those
    names, and compares that the conmaps generated for the same names are the
    same.
    """
    for _ in range(20):
        rands = [randomString() for _ in range(24)]
        conmaps = [distributedConnectionMap(rands) for _ in range(10)]
        for conmap1, conmap2 in combinations(conmaps, 2):
            assert conmap1 == conmap2


def test_list_item_equality():
    l = [
        {'a': 1, 'b': 2, 'c': 3},
        {'c': 3, 'a': 1, 'b': 2},
        {'c': 3, 'a': 1, 'b': 2},
        {'a': 1, 'b': 2, 'c': 3},
        {'c': 3, 'a': 1, 'b': 2},
        {'b': 2, 'c': 3, 'a': 1},
    ]
    l1 = [{'a', 'b', 'c', 1}, {'c', 'a', 'b', 1}, {1, 'a', 'c', 'b'}]
    assert check_if_all_equal_in_list(l)
    assert check_if_all_equal_in_list(l1)
    assert check_if_all_equal_in_list([1, 1, 1, 1])
    assert check_if_all_equal_in_list(['a', 'a', 'a', 'a'])
    assert not check_if_all_equal_in_list(['b', 'a', 'a', 'a'])
    assert not check_if_all_equal_in_list(l + [{'a': 1, 'b': 2, 'c': 33}])
    assert not check_if_all_equal_in_list(l1 + [{'c', 'a', 'b', 11}])


def test_3PC_key_comaparison():
    assert compare_3PC_keys((1, 2), (1, 2)) == 0
    assert compare_3PC_keys((1, 3), (1, 2)) < 0
    assert compare_3PC_keys((1, 2), (1, 3)) > 0
    assert compare_3PC_keys((1, 2), (1, 10)) > 0
    assert compare_3PC_keys((1, 100), (2, 3)) > 0
    assert compare_3PC_keys((1, 100), (4, 3)) > 0
    assert compare_3PC_keys((2, 100), (1, 300)) < 0
    assert min_3PC_key([(2, 100), (1, 300), (5, 600)]) == (1, 300)
    assert min_3PC_key([(2, 100), (2, 300), (2, 600)]) == (2, 100)
    assert min_3PC_key([(2, 100), (2, 300), (1, 600)]) == (1, 600)
    assert max_3PC_key([(2, 100), (1, 300), (5, 6)]) == (5, 6)
    assert max_3PC_key([(2, 100), (3, 20), (4, 1)]) == (4, 1)
    assert max_3PC_key([(2, 100), (2, 300), (2, 400)]) == (2, 400)


def test_utc_epoch():
    t1 = get_utc_epoch()
    time.sleep(1)
    t2 = get_utc_epoch()
    assert 1 <= t2 - t1 < 2

    old_tz = os.environ.get('TZ')

    t3 = get_utc_epoch()
    os.environ['TZ'] = 'Europe/London'
    time.tzset()
    time.sleep(1)
    t4 = get_utc_epoch()
    assert 1 <= t4 - t3 < 2

    t5 = get_utc_epoch()
    os.environ['TZ'] = 'America/St_Johns'
    time.tzset()
    time.sleep(1)
    t6 = get_utc_epoch()
    assert 1 <= t6 - t5 < 2

    if old_tz is None:
        del os.environ['TZ']
    else:
        os.environ['TZ'] = old_tz


def test_mostCommonElement_for_hashable():
    elements = [1, 2, 3, 3, 2, 5, 2]
    most_common, count = mostCommonElement(elements)
    assert most_common == 2
    assert count == 3


def test_mostCommonElement_for_non_hashable():
    elements = [{1: 2}, {1: 3}, {1: 4}, {1: 3}]
    most_common, count = mostCommonElement(elements)
    assert most_common == {1: 3}
    assert count == 2


def test_mostCommonElement_for_non_hashable_with_hashable_f():
    elements = [{1: 2}, {1: 3}, {1: 4}, {1: 3}]
    most_common, count = mostCommonElement(elements, lambda el: tuple(el.items()))
    assert most_common == {1: 3}
    assert count == 2


def test_mostCommonElement_for_mixed_hashable():
    elements = [{1: 2}, 4, {1: 3}, 4, {1: 4}, 4, {1: 3}]
    most_common, count = mostCommonElement(elements)
    assert most_common == 4
    assert count == 3
