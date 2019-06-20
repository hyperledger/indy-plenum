from functools import partial
from typing import Any

from plenum.common.event_bus import ExternalBus
from plenum.common.timer import TimerService
from plenum.test.simulation.sim_random import SimRandom


class SimNetwork:
    def __init__(self, timer: TimerService, random: SimRandom):
        self._timer = timer
        self._random = random
        self._min_latency = 1
        self._max_latency = 5
        self._peers = {}  # type: Dict[str, ExternalBus]

    def create_peer(self, name: str) -> ExternalBus:
        if name in self._peers:
            raise ValueError("Peer with name '{}' already exists".format(name))

        bus = ExternalBus(lambda msg, dst: self._send_message(name, msg, dst))
        self._peers[name] = bus
        return bus

    def _send_message(self, frm: str, msg: Any, dst: ExternalBus.Destination):
        if dst is None:
            dst = [name for name in self._peers if name != frm]
        elif isinstance(dst, str):
            dst = [dst]

        for name in dst:
            peer = self._peers.get(name)
            if not peer:
                # TODO: Log failed attempt
                continue

            self._timer.schedule(self._random.integer(self._min_latency, self._max_latency),
                                 partial(peer.process_incoming, msg, frm))
