from plenum.common.timer import RepeatingTimer
from plenum.test.helper import MockTimer


class Callback:
    def __init__(self):
        self.call_count = 0

    def __call__(self, *args, **kwargs):
        self.call_count += 1


def test_repeating_timer_is_started_active():
    timer = MockTimer()
    cb = Callback()
    RepeatingTimer(timer, 5, cb)
    assert cb.call_count == 0

    timer.advance(3)
    assert cb.call_count == 0

    timer.advance(3)
    assert cb.call_count == 1

    timer.advance(3)
    assert cb.call_count == 1

    timer.advance(3)
    assert cb.call_count == 2


def test_repeating_timer_triggers_callback_on_time():
    timer = MockTimer()
    cb = Callback()
    RepeatingTimer(timer, 5, cb)
    assert cb.call_count == 0

    timer.advance(5)
    assert cb.call_count == 1

    timer.advance(5)
    assert cb.call_count == 2


def test_repeating_timer_can_be_stopped():
    timer = MockTimer()
    cb = Callback()
    repeating_timer = RepeatingTimer(timer, 5, cb)

    assert cb.call_count == 0

    timer.advance(4)
    assert cb.call_count == 0

    timer.advance(4)
    assert cb.call_count == 1

    repeating_timer.stop()
    timer.advance(4)
    assert cb.call_count == 1

    timer.advance(4)
    assert cb.call_count == 1


def test_repeating_timer_can_be_started_inactive():
    timer = MockTimer()
    cb = Callback()
    RepeatingTimer(timer, 5, cb, active=False)

    assert cb.call_count == 0

    timer.advance(3)
    assert cb.call_count == 0

    timer.advance(3)
    assert cb.call_count == 0

    timer.advance(3)
    assert cb.call_count == 0

    timer.advance(3)
    assert cb.call_count == 0


def test_repeating_timer_can_be_stopped_and_started():
    timer = MockTimer()
    cb = Callback()
    repeating_timer = RepeatingTimer(timer, 5, cb)

    assert cb.call_count == 0

    timer.advance(4)
    assert cb.call_count == 0

    timer.advance(4)
    assert cb.call_count == 1

    repeating_timer.stop()
    timer.advance(4)
    assert cb.call_count == 1

    timer.advance(4)
    assert cb.call_count == 1

    repeating_timer.start()
    timer.advance(4)
    assert cb.call_count == 1

    timer.advance(4)
    assert cb.call_count == 2


def test_repeating_timer_doesnt_repeat_too_much():
    timer = MockTimer()
    cb = Callback()
    RepeatingTimer(timer, 5, cb)

    timer.advance(12)
    assert cb.call_count == 1

    timer.advance(12)
    assert cb.call_count == 2


def test_multiple_repeating_timers_can_work_together():
    timer = MockTimer()
    cb1 = Callback()
    cb2 = Callback()
    RepeatingTimer(timer, 5, cb1)
    RepeatingTimer(timer, 2, cb2)

    timer.advance(3)
    assert cb1.call_count == 0
    assert cb2.call_count == 1

    timer.advance(3)
    assert cb1.call_count == 1
    assert cb2.call_count == 2

    timer.advance(3)
    assert cb1.call_count == 1
    assert cb2.call_count == 3

    timer.advance(3)
    assert cb1.call_count == 2
    assert cb2.call_count == 4
