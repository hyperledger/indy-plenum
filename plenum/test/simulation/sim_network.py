from collections import OrderedDict
from functools import partial
from typing import Any, Iterable

from plenum.common.event_bus import ExternalBus
from plenum.common.timer import TimerService
from plenum.test.simulation.sim_random import SimRandom


class SimNetwork:
    def __init__(self, timer: TimerService, random: SimRandom):
        self._timer = timer
        self._random = random
        self._min_latency = 1
        self._max_latency = 500
        self._peers = OrderedDict()  # type: OrderedDict[str, ExternalBus]

    def create_peer(self, name: str) -> ExternalBus:
        if name in self._peers:
            raise ValueError("Peer with name '{}' already exists".format(name))

        bus = ExternalBus(partial(self._send_message, name))
        self._peers[name] = bus
        return bus

    def set_latency(self, min_value: int, max_value: int):
        self._min_latency = min_value
        self._max_latency = max_value

    def _send_message(self, frm: str, msg: Any, dst: ExternalBus.Destination):
        if dst is None:
            dst = [name for name in self._peers if name != frm]
        elif isinstance(dst, str):
            dst = [dst]
        elif isinstance(dst, Iterable):
            assert len(dst) > 0, "{} tried to send message {} to no one".format(frm, msg)
        else:
            assert False, "{} tried to send message {} to unsupported destination {}".format(frm, msg, dst)

        for name in dst:
            assert name != frm, "{} tried to send message {} to itself".format(frm, msg)

            peer = self._peers.get(name)
            assert peer, "{} tried to send message {} to unknown peer {}".format(frm, msg, name)

            self._timer.schedule(self._random.integer(self._min_latency, self._max_latency),
                                 partial(peer.process_incoming, msg, frm))
