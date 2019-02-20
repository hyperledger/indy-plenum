from typing import Callable, NamedTuple

from plenum.common.timer import TimerService
from plenum.simulation.sim_event_stream import ListEventStream, SimEvent
from plenum.simulation.sim_model import SimModel


class TimerModel(SimModel, TimerService):
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
        self._outbox.add(SimEvent(timestamp=self._ts + delay,
                                  payload=self.TimerEvent(callback=callback)))

    def cancel(self, callback: Callable):
        self._outbox.remove_all(lambda ev:
                                isinstance(ev.payload, self.TimerEvent) and
                                ev.payload.callback == callback)
