from abc import ABC, abstractmethod
from math import inf

from hypothesis import strategies as st
from hypothesis.strategies import composite
from typing import NamedTuple, Any, List, Optional, Set, Union, Iterable

ErrorEvent = NamedTuple("ErrorEvent", [("reason", str)])
AnyEvent = Union[ErrorEvent, Any]

SimEvent = NamedTuple("SimEvent", [("timestamp", int), ("payload", AnyEvent)])


@composite
def sim_events(draw, payload, min_size=0, max_size=20, min_interval=1, max_interval=100, start_ts=0):
    st_intervals = st.integers(min_value=min_interval, max_value=max_interval)
    st_event = st.tuples(st_intervals, payload)
    events = draw(st.lists(elements=st_event, min_size=min_size, max_size=max_size))

    ts = start_ts
    result = []
    for ev in events:
        ts += ev[0]
        result.append(SimEvent(timestamp=ts, payload=ev[1]))
    return result


class SimEventStream(ABC):
    @abstractmethod
    def advance(self, draw):
        pass

    @abstractmethod
    def peek(self) -> Optional[SimEvent]:
        pass


@composite
def sim_event_stream(draw, stream: SimEventStream, max_size=100):
    result = []
    for _ in range(max_size):
        event = stream.peek()
        if event is None:
            break
        result.append(event)
        if isinstance(event.payload, ErrorEvent):
            break
        stream.advance(draw)
    return result


class RandomEventStream(SimEventStream):
    def __init__(self, draw, events, min_interval=1, max_interval=100, start_ts=0):
        self._events = events
        self._interval = st.integers(min_value=min_interval, max_value=max_interval)
        self._ts = start_ts
        self.advance(draw)

    def advance(self, draw):
        self._ts += draw(self._interval)
        self._next_event = SimEvent(timestamp=self._ts, payload=draw(self._events))

    def peek(self) -> Optional[SimEvent]:
        return self._next_event


class ListEventStream(SimEventStream):
    def __init__(self, events: Iterable[SimEvent] = ()):
        self._events = [ev for ev in events]

    def extend(self, events):
        self._events.extend(events)
        self._events.sort(key=lambda ev: ev.timestamp)

    @property
    def events(self):
        return self._events[:]

    def advance(self, _):
        self._events.pop(0)

    def peek(self) -> Optional[SimEvent]:
        if len(self._events) > 0:
            return self._events[0]


class CompositeEventStream(SimEventStream):
    def __init__(self, *args):
        self._streams = [s for s in args]
        self._sort_streams()

    def advance(self, draw):
        self._streams[0].advance(draw)
        self._sort_streams()

    def peek(self) -> Optional[SimEvent]:
        return self._streams[0].peek()

    def _sort_streams(self):
        self._streams.sort(key=lambda s: self._stream_key(s))

    @staticmethod
    def _stream_key(s):
        ev = s.peek()
        if ev is None:
            return inf
        return ev.timestamp
