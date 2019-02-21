from abc import ABC, abstractmethod
from math import inf

from hypothesis import strategies as st
from hypothesis.strategies import composite
from typing import NamedTuple, Any, List, Optional, Set, Union, Iterable, Callable

from sortedcontainers import SortedListWithKey

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

    @abstractmethod
    def sort(self):
        pass

    def pop(self, draw) -> Optional[SimEvent]:
        result = self.peek()
        if result is not None:
            self.advance(draw)
        return result


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

    def sort(self):
        pass


class ListEventStream(SimEventStream):
    def __init__(self, events: Iterable[SimEvent] = ()):
        self._events = SortedListWithKey(iterable=events, key=lambda ev: ev.timestamp)

    def add(self, event):
        self._events.add(event)

    def extend(self, events):
        self._events.update(events)

    def remove_all(self, predicate: Callable):
        indexes = [i for i, ev in enumerate(self._events) if predicate(ev)]
        for i in reversed(indexes):
            del self._events[i]

    @property
    def events(self):
        return self._events[:]

    def advance(self, _):
        self._events.pop(0)

    def peek(self) -> Optional[SimEvent]:
        if len(self._events) > 0:
            return self._events[0]

    def sort(self):
        pass


class CompositeEventStream(SimEventStream):
    def __init__(self, *args):
        self._streams = [s for s in args]
        self.sort()

    def add_stream(self, stream):
        self._streams.append(stream)
        self.sort()

    def advance(self, draw):
        self._streams[0].advance(draw)
        self.sort()

    def peek(self) -> Optional[SimEvent]:
        return self._streams[0].peek()

    def sort(self):
        for stream in self._streams:
            stream.sort()
        self._streams.sort(key=lambda s: self._stream_key(s))

    @staticmethod
    def _stream_key(s):
        ev = s.peek()
        if ev is None:
            return inf
        return ev.timestamp
