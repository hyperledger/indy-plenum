from abc import ABC, abstractmethod
from typing import List

from sim_event_stream import SimEvent, SimEventStream, ListEventStream, CompositeEventStream


class SimModel(ABC):
    @abstractmethod
    def process(self, draw, event: SimEvent, is_stable: bool) -> List[SimEvent]:
        pass


class ModelEventStream(SimEventStream):
    def __init__(self, draw, model: SimModel, *args):
        self._model = model
        self._internal_events = ListEventStream()
        self._all_events = CompositeEventStream(self._internal_events, *args)

    def advance(self, draw):
        new_events = self._model.process(draw, self.peek(), self._is_stable)
        if new_events:
            self._internal_events.extend(new_events)
        self._all_events.advance(draw)

    def peek(self):
        return self._all_events.peek()

    @property
    def _is_stable(self):
        int_events_len = len(self._internal_events.events)
        if int_events_len == 0:
            return True
        if int_events_len == 1 and self._all_events._streams[0] == self._internal_events:
            return True
        return False
