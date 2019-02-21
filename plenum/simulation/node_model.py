from collections import defaultdict
from typing import NamedTuple, List, Any, Set

from plenum.common.timer import TimerService
from plenum.server.quorums import Quorums
from plenum.simulation.node_model_view_changer import create_view_changer
from plenum.simulation.pool_connections import PoolConnections
from plenum.simulation.sim_event_stream import ListEventStream, SimEvent, CompositeEventStream
from plenum.simulation.timer_model import TimerModel

Connect = NamedTuple('Connect', [])
Disconnect = NamedTuple('Disconnect', [])
CatchupDoneEvent = NamedTuple('CatchupDone', [('node_id', int)])
NetworkEvent = NamedTuple('NetworkEvent', [('src', int), ('dst', int), ('payload', Any)])


class NodeModel:
    def __init__(self, node_id: int, quorum: Quorums, connections: PoolConnections):
        self._id = node_id
        self._quorum = quorum
        self._ts = 0
        self._corrupted_id = None
        self._timer = TimerModel(self.id_to_name(node_id))
        self._connections = connections
        self._view_changer = create_view_changer(self)
        self._internal_outbox = ListEventStream()
        self.outbox = CompositeEventStream(self._internal_outbox, self._timer.outbox())

    @property
    def id(self):
        return self._id

    @property
    def view_no(self):
        return self._view_changer.view_no

    @property
    def is_participating(self):
        return not self._view_changer.view_change_in_progress and not self.is_corrupted

    @property
    def primary_id(self):
        return self._primary_id(self._view_changer.view_no)

    @property
    def next_primary_name(self):
        view_no = self._view_changer.view_no
        if not self._view_changer.view_change_in_progress:
            view_no += 1
        return self.id_to_name(self._primary_id(view_no))

    @property
    def current_primary_name(self):
        view_no = self._view_changer.view_no - 1
        if not self._view_changer.view_change_in_progress:
            view_no += 1
        return self.id_to_name(self._primary_id(view_no))

    @property
    def is_primary(self):
        return self.id == self.primary_id

    @property
    def is_primary_disconnected(self):
        return self._connections.are_connected(self._ts, (self.id, self.primary_id))

    @property
    def is_corrupted(self):
        return self._corrupted_id == self.id

    @property
    def connected_nodes(self) -> List[int]:
        result = []
        for id in range(1, self._quorum.n + 1):
            if id == self.id:
                continue
            if self._connections.are_connected(self._ts, (self.id, id)):
                result.append(id)
        return result

    def restart(self):
        pass

    def outage(self, other_id: int):
        if other_id == self.primary_id:
            self._view_changer.on_primary_loss()
            self._flush_viewchanger_outbox()

    def corrupt(self, node_id: int):
        self._corrupted_id = node_id
        if self._corrupted_id == self.primary_id:
            self._view_changer.on_master_degradation()
            self._flush_viewchanger_outbox()

    def schedule_finish_catchup(self):
        self._send(CatchupDoneEvent(node_id=self.id), delay=10)

    def process(self, draw, event: SimEvent):
        self._ts = event.timestamp
        self._timer.process(draw, event)

        if isinstance(event.payload, CatchupDoneEvent):
            if event.payload.node_id == self.id:
                self._view_changer.on_catchup_complete()

        if isinstance(event.payload, NetworkEvent):
            self.process_network(event.payload)

        self._flush_viewchanger_outbox()

    def process_network(self, message: NetworkEvent):
        if message.dst != self.id:
            return

        self._view_changer.inBoxRouter.handleSync((message.payload, self.id_to_name(message.src)))

    def _flush_viewchanger_outbox(self):
        for msg in self._view_changer.outBox:
            self._broadcast(msg)
        self._view_changer.outBox.clear()

    def _send(self, message, delay=1):
        self._internal_outbox.add(SimEvent(timestamp=self._ts + delay, payload=message))
        self.outbox.sort()

    def _broadcast(self, payload):
        for id in range(1, self._quorum.n + 1):
            if id == self.id:
                continue
            self._send(NetworkEvent(src=self.id, dst=id, payload=payload))

    def _primary_id(self, view_no):
        return 1 + view_no % self._quorum.n

    @staticmethod
    def id_to_name(id: int) -> str:
        return "Node{}".format(id)
