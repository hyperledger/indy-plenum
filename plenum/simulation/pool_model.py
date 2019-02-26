from collections import defaultdict
from typing import NamedTuple, Set, List, Optional

from plenum.server.quorums import Quorums
from plenum.simulation.node_model import NodeModel, NetworkEvent
from plenum.simulation.pool_connections import PoolConnections
from plenum.simulation.sim_event_stream import SimEvent, ErrorEvent, ListEventStream, CompositeEventStream
from plenum.simulation.sim_model import SimModel

RestartEvent = NamedTuple('RestartEvent', [('node', str)])
OutageEvent = NamedTuple('OutageEvent', [('node', str), ('disconnecteds', Set[str]), ('duration', int)])
CorruptEvent = NamedTuple('CorruptEvent', [('node', str)])


class PoolModel(SimModel):
    def __init__(self, nodes: List[str]):
        self._message_delay = 1
        self._node_names = nodes
        self._quorum = Quorums(len(nodes))
        self._connections = PoolConnections()
        self._nodes = {name: NodeModel(name, self._node_names, self._connections)
                       for name in self._node_names}
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
        participating = sum(1 for node in self._nodes.values() if node.is_participating)
        if not self._quorum.strong.is_reached(participating):
            return 'Not enough participating nodes for consensus'

        nodes_per_view = defaultdict(list)
        for node in self._nodes.values():
            nodes_per_view[node.view_no].append(node)
        try:
            nodes = next(nodes for nodes in nodes_per_view.values()
                         if self._quorum.strong.is_reached(len(nodes)))
        except StopIteration:
            return 'No strong quorum has same view'

        some_node = nodes[0]
        primary = self._nodes[some_node.primary_name]
        if primary.view_no != some_node.view_no:
            return 'Primary has different view'
        if not primary.is_participating:
            return 'Primary is not participating'

    def process_restart(self, ts: int, event: RestartEvent):
        restarting_node = self._nodes[event.node]
        restarting_node.restart()

        for node in self._nodes.values():
            if node != restarting_node:
                self._outage(ts, 5, restarting_node, node, process_node_a=False)

    def process_outage(self, ts: int, event: OutageEvent):
        outage_node = self._nodes[event.node]

        for node in event.disconnecteds:
            if node == event.node:
                continue
            node = self._nodes[node]
            self._outage(ts, event.duration, outage_node, node)

    def process_corrupt(self, ts: int, event: CorruptEvent):
        for node in self._nodes.values():
            node.corrupt(event.node)

    def check_status(self) -> Optional[ErrorEvent]:
        for node in self._nodes.values():
            if node.is_primary and not node.is_participating:
                return ErrorEvent('Cannot reelect primary')
        participating = sum(1 for node in self._nodes.values() if node.is_participating)
        if not self._quorum.strong.is_reached(participating):
            return ErrorEvent('Consensus lost')

    def _outage(self, ts: int, duration: int,
                node_a: NodeModel, node_b: NodeModel, process_node_a=True):
        node_ids = (node_a.name, node_b.name)
        if not self._connections.are_connected(ts, node_ids):
            return

        self._connections.disconnect_till(ts + duration, node_ids)
        if process_node_a:
            node_a.outage(node_b.name)
        node_b.outage(node_a.name)
