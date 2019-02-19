from plenum.common.timer import Timer, RepeatingTimer
from plenum.test.helper import MockTimestamp


class Callback:
    def __init__(self):
        self.call_count = 0

    def __call__(self, *args, **kwargs):
        self.call_count += 1


def test_timer_can_schedule_callback():
    ts = MockTimestamp(0)
    timer = Timer(ts)
    cb = Callback()

    timer.schedule(5, cb)
    assert cb.call_count == 0

    timer.service()
    assert cb.call_count == 0

    ts.value += 3
    timer.service()
    assert cb.call_count == 0

    ts.value += 3
    timer.service()
    assert cb.call_count == 1

    timer.service()
    assert cb.call_count == 1

    ts.value += 6
    timer.service()
    assert cb.call_count == 1


def test_timer_can_schedule_callback_after_first_call():
    ts = MockTimestamp(0)
    timer = Timer(ts)
    cb = Callback()

    timer.schedule(5, cb)
    ts.value += 6
    timer.service()
    assert cb.call_count == 1

    timer.schedule(5, cb)
    ts.value += 6
    timer.service()
    assert cb.call_count == 2


def test_timer_can_schedule_callback_twice():
    ts = MockTimestamp(0)
    timer = Timer(ts)
    cb = Callback()

    timer.schedule(3, cb)
    timer.schedule(5, cb)

    ts.value += 4
    timer.service()
    assert cb.call_count == 1

    ts.value += 4
    timer.service()
    assert cb.call_count == 2


def test_timer_can_schedule_and_process_callback_twice():
    ts = MockTimestamp(0)
    timer = Timer(ts)
    cb = Callback()

    timer.schedule(3, cb)
    timer.schedule(5, cb)

    ts.value += 6
    timer.service()
    assert cb.call_count == 2


def test_timer_can_schedule_same_callback_on_same_time_twice():
    ts = MockTimestamp(0)
    timer = Timer(ts)
    cb = Callback()

    timer.schedule(5, cb)
    timer.schedule(5, cb)

    ts.value += 6
    timer.service()
    assert cb.call_count == 2


def test_timer_can_schedule_different_callbacks_on_same_time_twice():
    ts = MockTimestamp(0)
    timer = Timer(ts)
    cb1 = Callback()
    cb2 = Callback()

    timer.schedule(5, cb1)
    timer.schedule(5, cb2)

    ts.value += 6
    timer.service()
    assert cb1.call_count == 1
    assert cb2.call_count == 1


def test_timer_can_schedule_different_callbacks():
    ts = MockTimestamp(0)
    timer = Timer(ts)
    cb1 = Callback()
    cb2 = Callback()

    timer.schedule(5, cb1)
    timer.schedule(3, cb2)

    ts.value += 4
    timer.service()
    assert cb1.call_count == 0
    assert cb2.call_count == 1

    ts.value += 4
    timer.service()
    assert cb1.call_count == 1
    assert cb2.call_count == 1


def test_timer_can_schedule_and_simultaneously_process_different_callbacks():
    ts = MockTimestamp(0)
    timer = Timer(ts)
    cb1 = Callback()
    cb2 = Callback()

    timer.schedule(5, cb1)
    timer.schedule(3, cb2)

    ts.value += 6
    timer.service()
    assert cb1.call_count == 1
    assert cb2.call_count == 1


def test_timer_can_cancel_callback():
    ts = MockTimestamp(0)
    timer = Timer(ts)
    cb = Callback()

    timer.schedule(5, cb)

    ts.value += 3
    timer.service()
    assert cb.call_count == 0

    timer.cancel(cb)

    ts.value += 3
    timer.service()
    assert cb.call_count == 0


def test_timer_cancel_callback_doesnt_crash_for_nonexistant_callback():
    ts = MockTimestamp(0)
    timer = Timer(ts)
    cb = Callback()

    # This shouldn't crash
    timer.cancel(cb)

    # Make sure that callback which was scheduled later is still called
    timer.schedule(5, cb)
    ts.value += 6
    timer.service()
    assert cb.call_count == 1

    # And this still shouldn't crash
    timer.cancel(cb)


def test_timer_can_cancel_callback_without_touching_other_callbacks():
    ts = MockTimestamp(0)
    timer = Timer(ts)
    cb1 = Callback()
    cb2 = Callback()
    cb3 = Callback()

    timer.schedule(5, cb1)
    timer.schedule(3, cb2)
    timer.schedule(4, cb3)
    timer.cancel(cb2)

    ts.value += 6
    timer.service()
    assert cb1.call_count == 1
    assert cb2.call_count == 0
    assert cb3.call_count == 1


def test_timer_cancels_all_instances_of_callback():
    ts = MockTimestamp(0)
    timer = Timer(ts)
    cb = Callback()

    timer.schedule(5, cb)
    timer.schedule(3, cb)
    timer.cancel(cb)

    ts.value += 6
    timer.service()
    assert cb.call_count == 0


def test_repeating_timer_is_started_active():
    period = 5
    ts = MockTimestamp(0)
    timer = Timer(ts)
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
    timer = Timer(ts)
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
    timer = Timer(ts)
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
    timer = Timer(ts)
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
    timer = Timer(ts)
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
    timer = Timer(ts)
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
