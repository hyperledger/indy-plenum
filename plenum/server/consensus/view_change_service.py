from _sha256 import sha256
from typing import List

from common.serializers.json_serializer import JsonSerializer
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck, NewView
from plenum.common.timer import TimerService
from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider
from plenum.server.quorums import Quorums


class ViewChangeVotesForView:
    """
    This is a container for view change votes for specific view
    """
    def __init__(self, quorums: Quorums):
        self._quorums = quorums

    @property
    def has_new_view_quorum(self) -> bool:
        return False

    def add_view_change(self, msg: ViewChange, frm: str):
        pass

    def add_view_change_ack(self, msg: ViewChangeAck, frm: str):
        pass


class ViewChangeService:
    def __init__(self, data: ConsensusDataProvider, timer: TimerService, bus: InternalBus, network: ExternalBus):
        self._data = data
        self._timer = timer
        self._bus = bus
        self._network = network

        network.subscribe(ViewChange, self.process_view_change_message)
        network.subscribe(ViewChangeAck, self.process_view_change_ack_message)
        network.subscribe(NewView, self.process_new_view_message)

    def start_view_change(self):
        # TODO: Calculate
        prepared = []
        preprepared = []

        self._data.view_no += 1
        self._data.waiting_for_new_view = True
        self._data.primary_name = self._find_primary(self._data.validators, self._data.view_no)

        vc = ViewChange(
            viewNo=self._data.view_no,
            stableCheckpoint=self._data.stable_checkpoint,
            prepared=prepared,
            preprepared=preprepared,
            checkpoints=self._data.checkpoints
        )
        self._network.send(vc)

    def process_view_change_message(self, msg: ViewChange, frm: str):
        # TODO: Validation

        # TODO: Probably we should stash messages from future view instead?
        if not self._is_primary(msg.viewNo):
            vca = ViewChangeAck(
                viewNo=msg.viewNo,
                name=frm,
                digest=self._view_change_digest(msg)
            )
            self._network.send(vca, self._data.primary_name)

    def process_view_change_ack_message(self, msg: ViewChangeAck, frm: str):
        # TODO: Validation
        if msg.viewNo != self._data.view_no:
            return

        if not self._data.is_primary:
            return

        nv = NewView(
            viewNo=msg.viewNo,
            viewChanges=[],
            checkpoint=None,
            preprepares=[]
        )
        self._network.send(nv)
        self._data.waiting_for_new_view = False

    def process_new_view_message(self, msg: NewView, frm: str):
        # TODO: Validation

        self._data.waiting_for_new_view = False

    @staticmethod
    def _view_change_digest(msg: ViewChange) -> str:
        serialized = JsonSerializer().dumps(msg.__dict__)
        return sha256(serialized).hexdigest()

    @staticmethod
    def _find_primary(validators: List[str], view_no: int) -> str:
        return validators[view_no % len(validators)]

    def _is_primary(self, view_no: int) -> bool:
        # TODO: Do we really need this?
        return self._find_primary(self._data.validators, view_no) == self._data.name
