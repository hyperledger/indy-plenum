from plenum.common.timer import QueueTimer, RepeatingTimer
from plenum.test.helper import MockTimestamp


class Callback:
    def __init__(self):
        self.call_count = 0

    def __call__(self, *args, **kwargs):
        self.call_count += 1


def test_repeating_timer_is_started_active():
    period = 5
    ts = MockTimestamp(0)
    timer = QueueTimer(ts)
    cb = Callback()
    repeating_timer = RepeatingTimer(timer, period, cb)
    assert cb.call_count == 0

    ts.value += 3
    timer.service()
    assert cb.call_count == 0

    ts.value += 3
    timer.service()
    assert cb.call_count == 1

    ts.value += 3
    timer.service()
    assert cb.call_count == 1

    ts.value += 3
    timer.service()
    assert cb.call_count == 2


def test_repeating_timer_can_be_stopped():
    ts = MockTimestamp(0)
    timer = QueueTimer(ts)
    cb = Callback()
    repeating_timer = RepeatingTimer(timer, 5, cb)

    assert cb.call_count == 0

    ts.value += 4
    timer.service()
    assert cb.call_count == 0

    ts.value += 4
    timer.service()
    assert cb.call_count == 1

    repeating_timer.stop()
    ts.value += 4
    timer.service()
    assert cb.call_count == 1

    ts.value += 4
    timer.service()
    assert cb.call_count == 1


def test_repeating_timer_can_be_started_inactive():
    ts = MockTimestamp(0)
    timer = QueueTimer(ts)
    cb = Callback()
    repeating_timer = RepeatingTimer(timer, 5, cb, active=False)

    assert cb.call_count == 0

    ts.value += 3
    timer.service()
    assert cb.call_count == 0

    ts.value += 3
    timer.service()
    assert cb.call_count == 0

    ts.value += 3
    timer.service()
    assert cb.call_count == 0

    ts.value += 3
    timer.service()
    assert cb.call_count == 0


def test_repeating_timer_can_be_stopped_and_started():
    ts = MockTimestamp(0)
    timer = QueueTimer(ts)
    cb = Callback()
    repeating_timer = RepeatingTimer(timer, 5, cb)

    assert cb.call_count == 0

    ts.value += 4
    timer.service()
    assert cb.call_count == 0

    ts.value += 4
    timer.service()
    assert cb.call_count == 1

    repeating_timer.stop()
    ts.value += 4
    timer.service()
    assert cb.call_count == 1

    ts.value += 4
    timer.service()
    assert cb.call_count == 1

    repeating_timer.start()
    ts.value += 4
    timer.service()
    assert cb.call_count == 1

    ts.value += 4
    timer.service()
    assert cb.call_count == 2


def test_repeating_timer_doesnt_repeat_too_much():
    ts = MockTimestamp(0)
    timer = QueueTimer(ts)
    cb = Callback()
    repeating_timer = RepeatingTimer(timer, 5, cb)

    ts.value += 12
    timer.service()
    assert cb.call_count == 1

    ts.value += 12
    timer.service()
    assert cb.call_count == 2


def test_multiple_repeating_timers_can_work_together():
    ts = MockTimestamp(0)
    timer = QueueTimer(ts)
    cb1 = Callback()
    cb2 = Callback()
    timer1 = RepeatingTimer(timer, 5, cb1)
    timer2 = RepeatingTimer(timer, 2, cb2)

    ts.value += 3
    timer.service()
    assert cb1.call_count == 0
    assert cb2.call_count == 1

    ts.value += 3
    timer.service()
    assert cb1.call_count == 1
    assert cb2.call_count == 2

    ts.value += 3
    timer.service()
    assert cb1.call_count == 1
    assert cb2.call_count == 3

    ts.value += 3
    timer.service()
    assert cb1.call_count == 2
    assert cb2.call_count == 4
