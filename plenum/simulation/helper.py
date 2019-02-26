from typing import NamedTuple
from hypothesis import strategies as st

from plenum.simulation.sim_event_stream import sim_events, ErrorEvent

MAX_EVENTS_SIZE = 20

SomeEvent = NamedTuple("SomeEvent", [("some_field", int)])


@st.composite
def some_event(draw):
    return SomeEvent(some_field=draw(st.integers()))


@st.composite
def some_events(draw, min_size=0, max_size=2 * MAX_EVENTS_SIZE,
                min_interval=1, max_interval=100, start_ts=0):
    return draw(sim_events(some_event(),
                           min_size=min_size, max_size=max_size,
                           min_interval=min_interval, max_interval=max_interval,
                           start_ts=start_ts))


def check_event_stream_invariants(events):
    # Timestamps should be monotonically increasing
    assert all(a.timestamp <= b.timestamp for a, b in zip(events, events[1:]))

    # Only one error should be possible
    num_errors = sum(1 for ev in events if isinstance(ev.payload, ErrorEvent))
    assert num_errors <= 1

    # If there is a error it should be last event
    if num_errors > 0:
        assert isinstance(events[-1].payload, ErrorEvent)
