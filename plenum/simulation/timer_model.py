from typing import Callable, NamedTuple, Optional

from plenum.common.timer import TimerService
from plenum.simulation.sim_event_stream import ListEventStream, SimEvent
from plenum.simulation.sim_model import SimModel


class TimerModel(SimModel, TimerService):
    class TimerEvent:
        def __init__(self, callback):
            self.callback = callback

        def __repr__(self):
            return 'TimerEvent({})'.format(str(self.callback))

    def __init__(self):
        self._ts = 0
        self._outbox = ListEventStream()

    def process(self, draw, event: SimEvent):
        self._ts = event.timestamp

        if isinstance(event.payload, self.TimerEvent):
            event.payload.callback()

    def outbox(self):
        return self._outbox

    def error_status(self) -> Optional[str]:
        pass

    def schedule(self, delay: float, callback: Callable):
        self._outbox.add(SimEvent(timestamp=self._ts + delay,
                                  payload=self.TimerEvent(callback)))

    def cancel(self, callback: Callable):
        self._outbox.remove_all(lambda ev:
                                isinstance(ev.payload, self.TimerEvent) and
                                ev.payload.callback == callback)
