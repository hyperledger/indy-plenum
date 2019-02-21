from typing import NamedTuple, Set, List, Optional

from plenum.server.quorums import Quorums
from plenum.simulation.node_model import NodeModel, NetworkEvent
from plenum.simulation.pool_connections import PoolConnections
from plenum.simulation.sim_event_stream import SimEvent, ErrorEvent, ListEventStream, CompositeEventStream
from plenum.simulation.sim_model import SimModel
from plenum.simulation.timer_model import TimerModel

RestartEvent = NamedTuple('RestartEvent', [('node_id', int)])
OutageEvent = NamedTuple('OutageEvent', [('node_id', int), ('disconnected_ids', Set[int]), ('duration', int)])
CorruptEvent = NamedTuple('CorruptEvent', [('node_id', int)])


class PoolModel(SimModel):
    def __init__(self, node_count: int):
        self._message_delay = 1
        self._quorum = Quorums(node_count)
        self._connections = PoolConnections()
        self._nodes = {id: NodeModel(id, self._quorum, self._connections)
                       for id in range(1, node_count + 1)}
        self._outbox = CompositeEventStream(*(node.outbox for node in self._nodes.values()))

    def process(self, draw, event: SimEvent):
        for node in self._nodes.values():
            node.process(draw, event)

        if isinstance(event.payload, RestartEvent):
            self.process_restart(event.timestamp, event.payload)

        if isinstance(event.payload, OutageEvent):
            self.process_outage(event.timestamp, event.payload)

        if isinstance(event.payload, CorruptEvent):
            self.process_corrupt(event.timestamp, event.payload)

    def outbox(self):
        return self._outbox

    def error_status(self) -> Optional[str]:
        for node in self._nodes.values():
            if node.is_primary and not node.is_participating:
                return 'Primary is not participating'
        participating = sum(1 for node in self._nodes.values() if node.is_participating)
        if not self._quorum.strong.is_reached(participating):
            return 'Not enough participating nodes for consensus'

    def process_restart(self, ts: int, event: RestartEvent):
        restarting_node = self._nodes[event.node_id]
        restarting_node.restart()

        for node in self._nodes.values():
            if node != restarting_node:
                self._outage(ts, 5, restarting_node, node, process_node_a=False)

    def process_outage(self, ts: int, event: OutageEvent):
        outage_node = self._nodes[event.node_id]

        for node_id in event.disconnected_ids:
            if node_id == event.node_id:
                continue
            node = self._nodes[node_id]
            self._outage(ts, event.duration, outage_node, node)

    def process_corrupt(self, ts: int, event: CorruptEvent):
        for node in self._nodes.values():
            node.corrupt(event.node_id)

    def check_status(self) -> Optional[ErrorEvent]:
        for node in self._nodes.values():
            if node.is_primary and not node.is_participating:
                return ErrorEvent('Cannot reelect primary')
        participating = sum(1 for node in self._nodes.values() if node.is_participating)
        if not self._quorum.strong.is_reached(participating):
            return ErrorEvent('Consensus lost')

    def _outage(self, ts: int, duration: int,
                node_a: NodeModel, node_b: NodeModel, process_node_a=True):
        node_ids = (node_a.id, node_b.id)
        if not self._connections.are_connected(ts, node_ids):
            return

        self._connections.disconnect_till(ts + duration, node_ids)
        if process_node_a:
            node_a.outage(node_b.id)
        node_b.outage(node_a.id)
