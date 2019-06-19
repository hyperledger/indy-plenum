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
            peers = [peer for peer in self._peers if peer != frm]
        elif isinstance(dst, str):
            peers = [dst]
        else:
            peers = dst

        peers = (self._peers[name] for name in peers if name in self._peers)
        for peer in peers:
            self._timer.schedule(self._random.integer(self._min_latency, self._max_latency),
                                 lambda: peer.process_incoming(msg, frm))
