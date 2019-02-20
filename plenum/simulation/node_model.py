from collections import defaultdict
from typing import NamedTuple, List, Any

from plenum.common.timer import TimerService
from plenum.server.quorums import Quorums
from plenum.simulation.node_model_view_changer import create_view_changer
from plenum.simulation.pool_connections import PoolConnections

Connect = NamedTuple('Connect', [])
Disconnect = NamedTuple('Disconnect', [])
InstanceChange = NamedTuple('InstanceChange', [('view_no', int), ('id', int)])
ViewChangeDone = NamedTuple('ViewChangeDone', [('view_no', int)])
NetworkEvent = NamedTuple('NetworkEvent', [('src', int), ('dst', int), ('payload', Any)])


class NodeModel:
    def __init__(self, node_id: int, quorum: Quorums, timer: TimerService, connections: PoolConnections):
        self._id = node_id
        self._quorum = quorum
        self._ts = 0
        self._view_no = 0
        self._view_change_in_progress = False
        self._corrupted_id = None
        self._instance_change_id = 0
        self._instance_change = defaultdict(set)
        self._view_change_done = defaultdict(set)
        self._timer = timer
        self._connections = connections
        self._view_changer = create_view_changer(self)

    @property
    def id(self):
        return self._id

    @property
    def view_no(self):
        return self._view_changer.view_no

    @property
    def is_participating(self):
        return not self._view_change_in_progress and not self.is_corrupted

    @property
    def primary_id(self):
        return self._primary_id(self.view_no)

    @property
    def is_primary(self):
        return self.id == self.primary_id

    @property
    def is_corrupted(self):
        return self._corrupted_id == self.id

    def need_view_change(self, view_no=None):
        if view_no is None:
            view_no = self.view_no
        primary_id = self._primary_id(view_no)
        if primary_id == self._corrupted_id:
            return True
        if not self._connections.are_connected(self._ts, (self.id, primary_id)):
            return True
        return False

    def update_ts(self, ts: int):
        self._ts = ts
        self._timer.service()

    def restart(self):
        # self._instance_change.clear()
        self._view_change_done.clear()
        self._instance_change_id = 0  # Comment to simulate persistence

    def outage(self, other_id: int) -> List[NetworkEvent]:
        if other_id != self.primary_id:
            return []
        return self._send_instance_change()

    def corrupt(self, node_id: int) -> List[NetworkEvent]:
        self._corrupted_id = node_id
        if not self.need_view_change():
            return []
        return self._send_instance_change()

    def process(self, message: NetworkEvent) -> List[NetworkEvent]:
        result = []
        if isinstance(message.payload, InstanceChange):
            result.extend(self.process_instance_change(message.src, message.payload))
        if isinstance(message.payload, ViewChangeDone):
            result.extend(self.process_view_change_done(message.src, message.payload))
        return result

    def process_instance_change(self, src: int, message: InstanceChange) -> List[NetworkEvent]:
        result = []

        if message.view_no <= self.view_no:
            return result

        if message.id < self._instance_change_id - 1:
            return result

        if message.id > self._instance_change_id - 1 and self.need_view_change(message.view_no - 1):
            self._instance_change_id = message.id
            result.extend(self._send_instance_change(message.view_no))

        self._instance_change[message].add(src)
        for k, v in self._instance_change.items():
            if k.view_no != self.view_no + 1:
                continue
            if not self._quorum.strong.is_reached(len(v)):
                continue
            self._view_no += 1
            self._view_change_in_progress = True
            self._instance_change_id = 0
            result.extend(self._broadcast(ViewChangeDone(view_no=self.view_no)))
            return result

        return result

    def process_view_change_done(self, src: int, message: ViewChangeDone) -> List[NetworkEvent]:
        self._view_change_done[message.view_no].add(src)
        if self._quorum.strong.is_reached(len(self._view_change_done[message.view_no])):
            self._view_no = message.view_no
            self._view_change_in_progress = False
            if self._corrupted_id == self.primary_id:
                return self._send_instance_change()
        return []

    def _send_instance_change(self, view_no=None) -> List[NetworkEvent]:
        if self.is_corrupted:
            return []
        if view_no is None:
            view_no = self.view_no + 1
        result = self._broadcast(InstanceChange(view_no=view_no, id=self._instance_change_id))
        # self._instance_change_id += 1
        return result

    def _broadcast(self, payload) -> List[NetworkEvent]:
        return [NetworkEvent(src=self.id, dst=id, payload=payload)
                for id in range(1, self._quorum.n + 1)]

    def _primary_id(self, view_no):
        return 1 + view_no % self._quorum.n
