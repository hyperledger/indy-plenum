from plenum.common.timer import RepeatingTimer
from plenum.simulation.sim_model import process_model
from plenum.simulation.timer_model import TimerModel

from hypothesis import strategies as st, given


class Callback():
    def __init__(self):
        self.call_count = 0

    def __call__(self):
        self.call_count += 1


@given(st.data())
def test_timer_model_can_schedule_event(data):
    timer = TimerModel()
    cb = Callback()

    timer.schedule(5, cb)
    assert cb.call_count == 0

    event_log = process_model(data.draw, timer)
    assert cb.call_count == 1
    assert len(event_log) == 1
    assert event_log[0].timestamp == 5


@given(st.data())
def test_timer_model_can_cancel_events(data):
    timer = TimerModel()
    cb1 = Callback()
    cb2 = Callback()
    cb3 = Callback()

    timer.schedule(5, cb1)
    timer.schedule(4, cb2)
    timer.schedule(3, cb3)
    timer.cancel(cb2)

    event_log = process_model(data.draw, timer)
    assert cb1.call_count == 1
    assert cb2.call_count == 0
    assert cb3.call_count == 1
    assert len(event_log) == 2
    assert event_log[0].timestamp == 3
    assert event_log[1].timestamp == 5


@given(st.data())
def test_timer_model_works_with_repeating_timers(data):
    timer = TimerModel()
    cb1 = Callback()
    cb2 = Callback()
    RepeatingTimer(timer, 5, cb1)
    RepeatingTimer(timer, 2, cb2)

    assert cb1.call_count == 0
    assert cb2.call_count == 0

    event_log = process_model(data.draw, timer, 1)
    assert cb1.call_count == 0
    assert cb2.call_count == 1
    assert len(event_log) == 1
    assert event_log[0].timestamp == 2

    event_log = process_model(data.draw, timer, 1)
    assert cb1.call_count == 0
    assert cb2.call_count == 2
    assert len(event_log) == 1
    assert event_log[0].timestamp == 4

    event_log = process_model(data.draw, timer, 1)
    assert cb1.call_count == 1
    assert cb2.call_count == 2
    assert len(event_log) == 1
    assert event_log[0].timestamp == 5

    event_log = process_model(data.draw, timer, 1)
    assert cb1.call_count == 1
    assert cb2.call_count == 3
    assert len(event_log) == 1
    assert event_log[0].timestamp == 6
