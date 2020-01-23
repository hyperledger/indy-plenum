from collections import OrderedDict
from contextlib import contextmanager
from functools import partial
from logging import getLogger
from typing import Any, Iterable, Optional, Callable, NamedTuple, Union, List, Tuple

from plenum.common.event_bus import ExternalBus
from plenum.common.timer import TimerService
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.test.simulation.sim_random import SimRandom


# TODO: INDY-2324 Should we namespace those definitions somehow?
Discard = NamedTuple('Discard', [('probability', float)])
Deliver = NamedTuple('Deliver', [('min_delay', float),
                                 ('max_delay', float)])
Stash = NamedTuple('Stash', [])
Action = Union[Discard, Deliver, Stash]

Selector = Callable[[Any, str, str], bool]  # predicate receiving message and names of source and destination peers
Processor = NamedTuple('Processor', [('action', Action),
                                     ('selectors', Iterable[Selector])])


def message_frm(frm: Union[str, Iterable[str]]) -> Selector:
    def _selector(_msg: Any, _frm: str, _dst: str):
        return _frm == frm if isinstance(frm, str) else _frm in frm
    return _selector


def message_dst(dst: Union[str, Iterable[str]]) -> Selector:
    def _selector(_msg: Any, _frm: str, _dst: str):
        return _dst == dst if isinstance(dst, str) else _dst in dst
    return _selector


def message_type(t: Union[type, Iterable[type]]) -> Selector:
    def _selector(_msg: Any, _frm: str, _dst: str):
        return isinstance(_msg, t) if isinstance(t, type) else isinstance(_msg, tuple(t))
    return _selector


class ProcessingChain:
    ScheduleDelivery = Callable[[float, Any, str, str], None]  # callable receiving delay, msg, source and destination peers

    def __init__(self, random: SimRandom, schedule_delivery: ScheduleDelivery):
        self._random = random
        self._schedule_delivery = schedule_delivery
        self._processors = []  # type: List[Processor]
        self._stashed_messages = []  # type: List[Tuple[Any, str, str]]
        self._default_min_latency = 0.01
        self._default_max_latency = 0.5

    def set_default_latency(self, min_value: float, max_value: float):
        self._default_min_latency = min_value
        self._default_max_latency = max_value

    def add(self, p: Processor):
        self._processors.append(p)

    def remove(self, p: Processor):
        if p not in self._processors:
            return
        self._processors.remove(p)
        self._pump_messages()

    def process(self, msg: Any, frm: str, dst: str):
        for p in reversed(self._processors):
            if not all(selector(msg, frm, dst) for selector in p.selectors):
                continue

            if isinstance(p.action, Discard):
                if self._random.float(0.0, 1.0) <= p.action.probability:
                    return
                else:
                    continue

            if isinstance(p.action, Stash):
                self._stashed_messages.append((msg, frm, dst))
                return

            if isinstance(p.action, Deliver):
                delay = self._random.float(p.action.min_delay, p.action.max_delay)
                self._schedule_delivery(delay, msg, frm, dst)
                return

        # By default apply default latency and deliver
        delay = self._random.float(self._default_min_latency, self._default_max_latency)
        self._schedule_delivery(delay, msg, frm, dst)

    def _pump_messages(self):
        messages_to_unstash = self._stashed_messages
        self._stashed_messages = []
        for msg, frm, dst in messages_to_unstash:
            self.process(msg, frm, dst)


class SimNetwork:
    def __init__(self,
                 timer: TimerService,
                 random: SimRandom,
                 serialize_deserialize: Optional[Callable] = None):
        self._timer = timer
        self._random = random
        self._serialize_deserialize = serialize_deserialize \
            if serialize_deserialize is not None \
            else lambda x: x
        self._logger = getLogger()
        self._peers = OrderedDict()  # type: OrderedDict[str, ExternalBus]
        self._processing_chain = ProcessingChain(random, self._schedule_delivery)

    def create_peer(self, name: str, handler=None) -> ExternalBus:
        if name in self._peers:
            raise ValueError("Peer with name '{}' already exists".format(name))

        handler = handler or partial(self._send_message, name)
        bus = ExternalBus(handler)
        self._peers[name] = bus
        return bus

    def add_processor(self, action: Action, *args: Selector) -> Processor:
        p = Processor(action=action, selectors=args)
        self._processing_chain.add(p)
        return p

    def remove_processor(self, p: Processor):
        self._processing_chain.remove(p)

    def set_latency(self, min_value: int, max_value: int):
        self._processing_chain.set_default_latency(min_value, max_value)

    def reset_filters(self, names: Iterable=None, messages_types: Iterable=None):
        if names is None:
            names = self._peers.keys()
        for name in names:
            if name not in self._filters:
                continue
            if messages_types is None:
                self._filters[name].clear()
                continue
            for msg_type in messages_types:
                self._filters[name].pop(msg_type, None)

    def _send_message(self, frm: str, msg: Any, dst: ExternalBus.Destination):
        if dst is None:
            dst = [name for name in self._peers if name != replica_name_to_node_name(frm)]
        elif isinstance(dst, str):
            dst = [dst]
        elif isinstance(dst, Iterable):
            assert len(dst) > 0, "{} tried to send message {} to no one".format(frm, msg)
        else:
            assert False, "{} tried to send message {} to unsupported destination {}".format(frm, msg, dst)

        msg = self._serialize_deserialize(msg)

        for name in sorted(dst):
            assert name != frm, "{} tried to send message {} to itself".format(frm, msg)

            assert isinstance(name, (str, bytes)), \
                "{} retied to send message {} to invalid peer {}".format(frm, msg, name)

            peer = self._peers.get(name)
            if peer is None:
                # ToDo: Remove this check after adding real node promoting
                # It can be possible after implementing INDY-2237 and INDY-2148
                self._logger.info("{} tried to send message {} to unknown peer {}".format(frm, msg, name))
                continue

            self._processing_chain.process(msg, frm, name)

    def _is_filtered(self, msg, name):
        message_type = type(msg)
        if name in self._filters and \
                self._filters[name].get(message_type, 0) * 100 >= self._random.integer(0, 100):
            self._logger.debug("Discard {} for {} because it filtered by SimNetwork".format(msg, name))
            return True
        return False

    def _schedule_delivery(self, delay: float, msg: Any, frm: str, dst: str):
        peer = self._peers.get(dst)
        if peer is None:
            self._logger.info("{} tried to send message {} to unknown peer {}".format(frm, msg, dst))
            return
        # assert peer, "{} tried to send message {} to unknown peer {}".format(frm, msg, name)
        self._timer.schedule(delay, partial(peer.process_incoming, msg, frm))
