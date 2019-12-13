from typing import NamedTuple, List, Any

from plenum.common.timer import RepeatingTimer
from plenum.server.quorums import Quorums
from plenum.simulation.node_model_view_changer import create_view_changer
from plenum.simulation.pool_connections import PoolConnections
from plenum.simulation.sim_event_stream import ListEventStream, SimEvent, CompositeEventStream
from plenum.simulation.timer_model import TimerModel

Connect = NamedTuple('Connect', [])
Disconnect = NamedTuple('Disconnect', [])
CatchupDoneEvent = NamedTuple('CatchupDone', [('node', str)])
NetworkEvent = NamedTuple('NetworkEvent', [('src', str), ('dst', str), ('payload', Any)])


class NodeModel:
    def __init__(self, name: str, node_names: List[str], connections: PoolConnections):
        self._name = name
        self._node_names = node_names
        self._quorum = Quorums(len(node_names))
        self._ts = 0
        self._corrupted_name = None
        self._timer = TimerModel(name)
        self._connections = connections
        self._view_changer = create_view_changer(self)
        self._internal_outbox = ListEventStream()
        self.outbox = CompositeEventStream(self._internal_outbox, self._timer.outbox())
        self._check_performance_timer = RepeatingTimer(self._timer, 30, self._check_performance)

    @property
    def name(self):
        return self._name

    @property
    def view_no(self):
        return self._view_changer.view_no

    @property
    def is_participating(self):
        return not self._view_changer.view_change_in_progress and not self.is_corrupted

    @property
    def primary_name(self):
        return self._primary_name(self._view_changer.view_no)

    @property
    def next_primary_name(self):
        view_no = self._view_changer.view_no
        if not self._view_changer.view_change_in_progress:
            view_no += 1
        return self._primary_name(view_no)

    @property
    def current_primary_name(self):
        view_no = self._view_changer.view_no - 1
        if not self._view_changer.view_change_in_progress:
            view_no += 1
        return self._primary_name(view_no)

    @property
    def is_primary(self):
        return self.name == self.primary_name

    @property
    def is_primary_disconnected(self):
        return self._connections.are_connected(self._ts, (self.name, self.primary_name))

    @property
    def is_corrupted(self):
        return self._corrupted_name == self.name

    @property
    def connected_nodes(self) -> List[int]:
        result = []
        for name in self._node_names:
            if name == self.name:
                continue
            if self._connections.are_connected(self._ts, (self.name, name)):
                result.append(name)
        return result

    def restart(self):
        pass

    def outage(self, node: str):
        if node == self.primary_name:
            self._view_changer.on_primary_loss()
            self._flush_viewchanger_outbox()

    def corrupt(self, node: str):
        self._corrupted_name = node
        if self.name == node:
            self._check_performance_timer.stop()

    def process(self, draw, event: SimEvent):
        self._ts = event.timestamp
        self._timer.process(draw, event)

        if isinstance(event.payload, CatchupDoneEvent):
            if event.payload.node == self.name:
                self._view_changer.on_catchup_complete()

        if isinstance(event.payload, NetworkEvent):
            self.process_network(event.payload)

        self._flush_viewchanger_outbox()

    def process_network(self, message: NetworkEvent):
        if self.name == self._corrupted_name:
            return
        if message.dst != self.name:
            return
        self._view_changer.inBoxRouter.handleSync((message.payload, message.src))

    def _flush_viewchanger_outbox(self):
        if self.name == self._corrupted_name:
            return
        for msg in self._view_changer.outBox:
            self._broadcast(msg)
        self._view_changer.outBox.clear()

    def _send(self, message, delay=1):
        self._internal_outbox.add(SimEvent(timestamp=self._ts + delay, payload=message))
        self.outbox.sort()

    def _broadcast(self, payload):
        for name in self._node_names:
            if name == self.name:
                continue
            self._send(NetworkEvent(src=self.name,
                                    dst=name,
                                    payload=payload))

    def _check_performance(self):
        if self.primary_name == self._corrupted_name:
            self._view_changer.on_master_degradation()
            self._flush_viewchanger_outbox()

    def _primary_name(self, view_no):
        return self._node_names[view_no % len(self._node_names)]
