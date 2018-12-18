import time
from collections import deque

import pytest

from plenum.test.stasher import Stasher, delay_rules


def delay_twos(item):
    if item == 2:
        return 2


def delay_threes(item):
    if item == 3:
        return 2


def delay_all(item):
    return 10000


@pytest.mark.skipif('sys.platform == "win32"', reason='SOV-457')
def test_delay():
    q = deque([1, 2, 3])
    s = Stasher(q, "my-stasher")

    # Check that relevant items are stashed from deque
    s.delay(delay_twos)
    s.process()
    assert list(q) == [1, 3]

    # Pretend that we processed items that are not stashed
    q.clear()

    # Check that nothing happened after one second
    time.sleep(1)
    s.process()
    assert list(q) == []

    # Check that stashed items returned to deque after one more second
    time.sleep(1)
    s.process()
    assert list(q) == [2]

    # Check that items are no longer stashed when delays are reset
    s.resetDelays()
    s.process()
    assert list(q) == [2]


def test_delay_rules_enable_delays_on_entry_and_disables_them_on_exit():
    s = Stasher(deque())

    with delay_rules(s, delay_twos):
        assert delay_twos in s.delayRules

    assert delay_twos not in s.delayRules


def test_delay_rules_dont_touch_other_delays():
    s = Stasher(deque())
    s.delay(delay_threes)

    with delay_rules(s, delay_twos):
        assert delay_threes in s.delayRules

    assert delay_threes in s.delayRules


def test_delay_rules_return_delayed_items_to_list_on_exit():
    q = deque([1, 2, 3])
    s = Stasher(q)
    s.delay(delay_threes)

    with delay_rules(s, delay_twos):
        s.process()
        assert 1 in q
        assert 2 not in q
        assert 3 not in q

    assert 1 in q
    assert 2 in q
    assert 3 not in q


def test_delay_rules_can_use_multiple_delayers():
    s = Stasher(deque())

    with delay_rules(s, delay_twos, delay_threes):
        assert delay_twos in s.delayRules
        assert delay_threes in s.delayRules

    assert delay_twos not in s.delayRules
    assert delay_threes not in s.delayRules


def test_delay_rules_can_use_multiple_stashers():
    s1 = Stasher(deque())
    s2 = Stasher(deque())

    with delay_rules([s1, s2], delay_twos):
        assert delay_twos in s1.delayRules
        assert delay_twos in s2.delayRules

    assert delay_twos not in s1.delayRules
    assert delay_twos not in s2.delayRules


def test_delay_rules_can_use_generator_expressions():
    stashers = [Stasher(deque(), name="{}".format(i)) for i in range(3)]

    with delay_rules((s for s in stashers if s.name != "1"), delay_twos):
        assert delay_twos in stashers[0].delayRules
        assert delay_twos not in stashers[1].delayRules
        assert delay_twos in stashers[2].delayRules

    assert delay_twos not in stashers[0].delayRules
    assert delay_twos not in stashers[1].delayRules
    assert delay_twos not in stashers[2].delayRules


def test_delay_rules_raise_type_error_when_given_stashers_of_wrong_type():
    with pytest.raises(TypeError):
        with delay_rules(1, delay_twos):
            pass

    with pytest.raises(TypeError):
        with delay_rules([1, 2], delay_twos):
            pass

    with pytest.raises(TypeError):
        with delay_rules(range(3), delay_twos):
            pass


def test_unstash_appends_to_right():
    q = deque([1, 2, 3])
    s = Stasher(q, "my-stasher")

    s.delay(delay_all)
    s.process()
    assert len(q) == 0

    s.force_unstash()
    assert q == deque([1, 2, 3])
