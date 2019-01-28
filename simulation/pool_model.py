from collections import defaultdict
from typing import NamedTuple, Set, Tuple, List, Optional

from node_model import Quorum, NodeModel, NetworkEvent
from sim_event_stream import SimEvent, ErrorEvent
from sim_model import SimModel

RestartEvent = NamedTuple('RestartEvent', [('node_id', int)])
OutageEvent = NamedTuple('OutageEvent', [('node_id', int), ('disconnected_ids', Set[int]), ('duration', int)])
CorruptEvent = NamedTuple('CorruptEvent', [('node_id', int)])


class PoolConnections:
    def __init__(self):
        self._disconnected_till = defaultdict(int)

    def are_connected(self, ts: int, node_ids: Tuple[int, int]):
        node_ids = self._normalize_ids(node_ids)
        return ts >= self._disconnected_till[node_ids]

    def disconnect_till(self, ts: int, node_ids: Tuple[int, int]):
        node_ids = self._normalize_ids(node_ids)
        ts = max(ts, self._disconnected_till[node_ids])
        self._disconnected_till[node_ids] = ts

    @staticmethod
    def _normalize_ids(ids: Tuple[int, int]):
        if ids[0] > ids[1]:
            return (ids[1], ids[0])
        return ids


class PoolModel(SimModel):
    def __init__(self, node_count):
        self._message_delay = 1
        self._quorum = Quorum(node_count)
        self._nodes = {id: NodeModel(id, self._quorum) for id in range(1, node_count + 1)}
        self._connections = PoolConnections()

    def process(self, draw, event: SimEvent, is_stable: bool) -> List[SimEvent]:
        result = []

        if isinstance(event.payload, RestartEvent):
            result.extend(self.process_restart(event.timestamp, event.payload))

        if isinstance(event.payload, OutageEvent):
            result.extend(self.process_outage(event.timestamp, event.payload))

        if isinstance(event.payload, CorruptEvent):
            result.extend(self.process_corrupt(event.timestamp, event.payload))

        if isinstance(event.payload, NetworkEvent):
            result.extend(self.process_network(event.timestamp, event.payload))

        if len(result) == 0 and is_stable:
            error = self.check_status()
            if error is not None:
                result.append(SimEvent(timestamp=event.timestamp, payload=error))

        return result

    def process_restart(self, ts: int, event: RestartEvent) -> List[SimEvent]:
        restarting_node = self._nodes[event.node_id]
        restarting_node.restart()

        result = []
        for node in self._nodes.values():
            if node != restarting_node:
                result.extend(self._outage(ts, 5, restarting_node, node, process_node_a=False))

        return result

    def process_outage(self, ts: int, event: OutageEvent) -> List[SimEvent]:
        outage_node = self._nodes[event.node_id]

        result = []
        for node_id in event.disconnected_ids:
            if node_id == event.node_id:
                continue
            node = self._nodes[node_id]
            result.extend(self._outage(ts, event.duration, outage_node, node))

        return result

    def process_corrupt(self, ts: int, event: CorruptEvent) -> List[SimEvent]:
        result = []
        for node in self._nodes.values():
            result.extend(SimEvent(ts + self._message_delay, msg) for msg in node.corrupt(event.node_id))
        return result

    def process_network(self, ts: int, message: NetworkEvent) -> List[SimEvent]:
        # if not self._connections.are_connected(ts, (message.src, message.dst)):
        #     return []
        node = self._nodes[message.dst]
        return [SimEvent(ts + self._message_delay, msg) for msg in node.process(message)]

    def check_status(self) -> Optional[ErrorEvent]:
        for node in self._nodes.values():
            if node.is_primary and not node.is_participating:
                return ErrorEvent('Cannot reelect primary')
        participating = sum(1 for node in self._nodes.values() if node.is_participating)
        if participating < self._quorum.strong:
            return ErrorEvent('Consensus lost')

    def _outage(self, ts: int, duration: int,
                node_a: NodeModel, node_b: NodeModel, process_node_a=True) -> List[SimEvent]:
        result = []
        node_ids = (node_a.id, node_b.id)
        if not self._connections.are_connected(ts, node_ids):
            return result

        self._connections.disconnect_till(ts + duration, node_ids)
        if process_node_a:
            result.extend(SimEvent(ts + self._message_delay, msg) for msg in node_a.outage(node_b.id))
        result.extend(SimEvent(ts + self._message_delay, msg) for msg in node_b.outage(node_a.id))
        return result
