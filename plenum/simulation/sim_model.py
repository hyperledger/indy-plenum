from abc import ABC, abstractmethod
from typing import List

from plenum.simulation.sim_event_stream import SimEvent, SimEventStream, CompositeEventStream, ErrorEvent


class SimModel(ABC):
    @abstractmethod
    def process(self, draw, event: SimEvent):
        pass

    @abstractmethod
    def outbox(self) -> SimEventStream:
        pass


class ModelWithExternalEvents(SimModel):
    def __init__(self, model: SimModel, events: SimEventStream):
        self._model = model
        self._external_events = events
        self._events = CompositeEventStream(model.outbox(), events)

    def process(self, draw, event: SimEvent):
        self._model.process(draw, event)

    def outbox(self) -> SimEventStream:
        return self._events


def process_model(draw, model: SimModel, max_size: int = 200) -> List[SimEvent]:
    event_log = []
    for _ in range(max_size):
        event = model.outbox().pop(draw)
        if event is None:
            break
        event_log.append(event)
        if isinstance(event.payload, ErrorEvent):
            break
        model.process(draw, event)
        model.outbox().sort()
    return event_log
