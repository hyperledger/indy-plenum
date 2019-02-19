from abc import ABC, abstractmethod
from typing import Callable, NamedTuple, List

from plenum.common.timer import TimerService
from plenum.simulation.sim_event_stream import ListEventStream, SimEvent, SimEventStream, CompositeEventStream, \
    ErrorEvent


class SimModel2(ABC):
    @abstractmethod
    def process(self, draw, event: SimEvent):
        pass

    @abstractmethod
    def outbox(self) -> SimEventStream:
        pass


class ModelWithExternalEvents(SimModel2):
    def __init__(self, model: SimModel2, events: SimEventStream):
        self._model = model
        self._external_events = events
        self._events = CompositeEventStream(model.outbox(), events)

    def process(self, draw, event: SimEvent):
        self._model.process(draw, event)

    def outbox(self) -> SimEventStream:
        return self._events


def process_model(draw, model: SimModel2, max_events: int = 200) -> List[SimEvent]:
    event_log = []
    for _ in range(max_events):
        event = model.outbox().pop(draw)
        if event is None:
            break
        event_log.append(event)
        if isinstance(event.payload, ErrorEvent):
            break
        model.process(draw, event)
    return event_log


class TimerModel(SimModel2, TimerService):
    TimerEvent = NamedTuple('TimerEvent', [('callback', Callable)])

    def __init__(self):
        self._ts = 0
        self._outbox = ListEventStream()

    def process(self, draw, event: SimEvent):
        self._ts = event.timestamp

        if isinstance(event.payload, self.TimerEvent):
            event.payload.callback()

    def outbox(self):
        return self._outbox

    def schedule(self, delay: float, callback: Callable):
        self._outbox.extend([SimEvent(timestamp=self._ts + delay,
                                      payload=self.TimerEvent(callback=callback))])

    def cancel(self, callback: Callable):
        self._outbox.remove_all(lambda ev:
                                isinstance(ev.payload, self.TimerEvent) and ev.payload.callback == callback)
