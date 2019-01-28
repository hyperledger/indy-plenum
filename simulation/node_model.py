from collections import defaultdict
from typing import NamedTuple, List, Any


class Quorum:
    def __init__(self, node_count: int):
        self.N = node_count
        self.f = (self.N - 1) // 3
        self.weak = self.f + 1
        self.strong = self.N - self.f


Connect = NamedTuple('Connect', [])
Disconnect = NamedTuple('Disconnect', [])
InstanceChange = NamedTuple('InstanceChange', [('view_no', int), ('id', int)])
ViewChangeDone = NamedTuple('ViewChangeDone', [('view_no', int)])
NetworkEvent = NamedTuple('NetworkEvent', [('src', int), ('dst', int), ('payload', Any)])


class NodeModel:
    def __init__(self, node_id: int, quorum: Quorum):
        self._id = node_id
        self._quorum = quorum
        self._view_no = 0
        self._view_change_in_progress = False
        self._corrupted_id = None
        self._instance_change_id = 0
        self._instance_change = defaultdict(set)
        self._view_change_done = defaultdict(set)

    @property
    def id(self):
        return self._id

    @property
    def view_no(self):
        return self._view_no

    @property
    def is_participating(self):
        return not self._view_change_in_progress and not self.is_corrupted

    @property
    def primary_id(self):
        return 1 + self.view_no % self._quorum.N

    @property
    def is_primary(self):
        return self.id == self.primary_id

    @property
    def is_corrupted(self):
        return self._corrupted_id == self.id

    def restart(self):
        self._instance_change.clear()
        self._view_change_done.clear()
        self._instance_change_id = 0  # Comment to simulate persistence

    def outage(self, other_id: int) -> List[NetworkEvent]:
        if other_id != self.primary_id:
            return []
        return self._send_instance_change()

    def corrupt(self, id) -> List[NetworkEvent]:
        self._corrupted_id = id
        if self._corrupted_id == self.primary_id:
            return self._send_instance_change()
        return []

    def process(self, message: NetworkEvent) -> List[NetworkEvent]:
        result = []
        if isinstance(message.payload, InstanceChange):
            result.extend(self.process_instance_change(message.src, message.payload))
        if isinstance(message.payload, ViewChangeDone):
            result.extend(self.process_view_change_done(message.src, message.payload))
        return result

    def process_instance_change(self, src: int, message: InstanceChange) -> List[NetworkEvent]:
        # if message.id < self._instance_change_id - 1:
        #     return []
        self._instance_change[message].add(src)
        for k, v in self._instance_change.items():
            if k.view_no != self.view_no + 1:
                continue
            if len(v) < self._quorum.strong:
                continue
            self._view_no += 1
            self._view_change_in_progress = True
            self._instance_change_id = 0
            return self._broadcast(ViewChangeDone(view_no=self.view_no))
        return []

    def process_view_change_done(self, src: int, message: ViewChangeDone) -> List[NetworkEvent]:
        self._view_change_done[message.view_no].add(src)
        if len(self._view_change_done[message.view_no]) >= self._quorum.strong:
            self._view_no = message.view_no
            self._view_change_in_progress = False
            if self._corrupted_id == self.primary_id:
                return self._send_instance_change()
        return []

    def _send_instance_change(self) -> List[NetworkEvent]:
        if self.is_corrupted:
            return []
        result = self._broadcast(InstanceChange(view_no=self.view_no + 1, id=self._instance_change_id))
        self._instance_change_id += 1
        return result

    def _broadcast(self, payload) -> List[NetworkEvent]:
        return [NetworkEvent(src=self.id, dst=id, payload=payload)
                for id in range(1, self._quorum.N + 1)]
