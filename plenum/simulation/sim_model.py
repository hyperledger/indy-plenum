from abc import ABC, abstractmethod
from typing import List, Optional

from plenum.simulation.sim_event_stream import SimEvent, SimEventStream, CompositeEventStream, ErrorEvent


class SimModel(ABC):
    @abstractmethod
    def process(self, draw, event: SimEvent):
        pass

    @abstractmethod
    def outbox(self) -> SimEventStream:
        pass

    @abstractmethod
    def error_status(self) -> Optional[str]:
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

    def error_status(self) -> Optional[str]:
        return self._model.error_status()


def process_model(draw, model: SimModel,
                  max_ok_rounds: int = 100,
                  max_err_rounds: int = 50,
                  max_size: int = 200) -> List[SimEvent]:
    event_log = []
    ok_rounds = 0
    err_rounds = 0
    for _ in range(max_size):
        event = model.outbox().pop(draw)
        if event is None:
            break
        event_log.append(event)
        if isinstance(event.payload, ErrorEvent):
            break
        model.process(draw, event)
        model.outbox().sort()

        if model.error_status() is None:
            ok_rounds += 1
            err_rounds = 0
            if ok_rounds == max_ok_rounds:
                break
        else:
            ok_rounds = 0
            err_rounds += 1
            if err_rounds == max_err_rounds:
                break

    return event_log
