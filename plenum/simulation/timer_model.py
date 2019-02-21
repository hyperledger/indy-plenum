from typing import Callable, NamedTuple, Optional

from plenum.common.timer import TimerService
from plenum.simulation.sim_event_stream import ListEventStream, SimEvent
from plenum.simulation.sim_model import SimModel


class TimerModel(SimModel, TimerService):
    TimerEvent = NamedTuple('TimerEvent', [('name', str), ('callback', Callable)])

    def __init__(self, name):
        self._name = name
        self._ts = 0
        self._outbox = ListEventStream()

    def process(self, draw, event: SimEvent):
        self._ts = event.timestamp

        if isinstance(event.payload, self.TimerEvent):
            if event.payload.name == self._name:
                event.payload.callback()

    def outbox(self):
        return self._outbox

    def error_status(self) -> Optional[str]:
        pass

    def get_current_time(self) -> float:
        return self._ts

    def schedule(self, delay: float, callback: Callable):
        # TODO: Some classes (like ViewChanger) sometimes try to schedule None events o_O
        if callback is None:
            return
        self._outbox.add(SimEvent(timestamp=self._ts + delay,
                                  payload=self.create_timer_event(callback)))

    def cancel(self, callback: Callable):
        self._outbox.remove_all(lambda ev:
                                isinstance(ev.payload, self.TimerEvent) and
                                ev.payload == self.create_timer_event(callback))

    def create_timer_event(self, callback: Callable):
        return self.TimerEvent(name=self._name, callback=callback)
