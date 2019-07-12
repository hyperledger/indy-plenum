from _sha256 import sha256
from collections import defaultdict
from functools import partial
from typing import List, Optional, Union

from common.serializers.json_serializer import JsonSerializer
from plenum.common.config_util import getConfig
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck, NewView
from plenum.common.stashing_router import StashingRouter, PROCESS, DISCARD, STASH
from plenum.common.timer import TimerService
from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider
from plenum.server.quorums import Quorums
from stp_core.common.log import getlogger


def view_change_digest(msg: ViewChange) -> str:
    serialized = JsonSerializer().dumps(msg.__dict__)
    return sha256(serialized).hexdigest()


class ViewChangeVotesForNode:
    """
    Storage for view change vote from some node for some view + corresponding acks
    """

    def __init__(self, quorums: Quorums):
        self._quorums = quorums
        self._view_change = None
        self._digest = None
        self._acks = defaultdict(set)  # Dict[str, Set[str]]

    @property
    def view_change(self) -> Optional[ViewChange]:
        """
        Returns received view change
        """
        return self._view_change

    @property
    def is_confirmed(self) -> bool:
        """
        Returns True if received view change message and enough corresponding acks
        """
        if self._digest is None:
            return False

        return self._quorums.view_change_ack.is_reached(len(self._acks[self._digest]))

    def add_view_change(self, msg: ViewChange) -> bool:
        """
        Adds view change vote and returns boolean indicating if it found node suspicios
        """
        if self._view_change is None:
            self._view_change = msg
            self._digest = view_change_digest(msg)
            return self._validate_acks()

        return self._digest == view_change_digest(msg)

    def add_view_change_ack(self, msg: ViewChangeAck, frm: str) -> bool:
        """
        Adds view change ack and returns boolean indicating if it found node suspicios
        """
        self._acks[msg.digest].add(frm)
        return self._validate_acks()

    def _validate_acks(self) -> bool:
        digests = [digest for digest, acks in self._acks.items()
                   if self._quorums.weak.is_reached(len(acks))]

        if len(digests) > 1:
            return False

        if len(digests) < 1 or self._digest is None:
            return True

        return self._digest == digests[0]


class ViewChangeVotesForView:
    """
    Storage for view change votes for some view + corresponding acks
    """

    def __init__(self, quorums: Quorums):
        self._quorums = quorums
        self._votes = defaultdict(partial(ViewChangeVotesForNode, quorums))

    @property
    def confirmed_votes(self) -> List[ViewChange]:
        return [node_votes.view_change for node_votes in self._votes.values()
                if node_votes.is_confirmed]

    @property
    def has_view_change_quorum(self) -> bool:
        return self._quorums.view_change.is_reached(len(self.confirmed_votes))

    def add_view_change(self, msg: ViewChange, frm: str) -> bool:
        """
        Adds view change ack and returns boolean indicating if it found node suspicios
        """
        return self._votes[frm].add_view_change(msg)

    def add_view_change_ack(self, msg: ViewChangeAck, frm: str) -> bool:
        """
        Adds view change ack and returns boolean indicating if it found node suspicios
        """
        return self._votes[msg.name].add_view_change_ack(msg, frm)

    def clear(self):
        self._votes.clear()


class ViewChangeService:
    def __init__(self, data: ConsensusDataProvider, timer: TimerService, bus: InternalBus, network: ExternalBus):
        self._config = getConfig()
        self._logger = getlogger()

        self._data = data
        self._timer = timer
        self._bus = bus
        self._network = network
        self._stasher = StashingRouter(self._config.VIEW_CHANGE_SERVICE_STASH_LIMIT)
        self._votes = ViewChangeVotesForView(self._data.quorums)

        self._stasher.subscribe(ViewChange, self.process_view_change_message)
        self._stasher.subscribe(ViewChangeAck, self.process_view_change_ack_message)
        self._stasher.subscribe(NewView, self.process_new_view_message)
        self._stasher.subscribe_to(network)

    def start_view_change(self, view_no: Optional[int] = None):
        if view_no is None:
            view_no = self._data.view_no + 1

        # TODO: Calculate
        prepared = []
        preprepared = []

        self._data.view_no = view_no
        self._data.waiting_for_new_view = True
        self._data.primary_name = self._find_primary(self._data.validators, self._data.view_no)
        self._votes.clear()

        vc = ViewChange(
            viewNo=self._data.view_no,
            stableCheckpoint=self._data.stable_checkpoint,
            prepared=prepared,
            preprepared=preprepared,
            checkpoints=self._data.checkpoints
        )
        self._network.send(vc)

        self._stasher.process_all_stashed()

    def process_view_change_message(self, msg: ViewChange, frm: str):
        result = self._validate(msg, frm)
        if result != PROCESS:
            return result

        self._votes.add_view_change(msg, frm)

        if self._data.is_primary:
            self._send_new_view_if_needed()
            return

        vca = ViewChangeAck(
            viewNo=msg.viewNo,
            name=frm,
            digest=view_change_digest(msg)
        )
        self._network.send(vca, self._data.primary_name)

    def process_view_change_ack_message(self, msg: ViewChangeAck, frm: str):
        result = self._validate(msg, frm)
        if result != PROCESS:
            return result

        if not self._data.is_primary:
            return

        self._votes.add_view_change_ack(msg, frm)
        self._send_new_view_if_needed()

    def process_new_view_message(self, msg: NewView, frm: str):
        result = self._validate(msg, frm)
        if result != PROCESS:
            return result

        self._data.waiting_for_new_view = False

    @staticmethod
    def _find_primary(validators: List[str], view_no: int) -> str:
        return validators[view_no % len(validators)]

    def _is_primary(self, view_no: int) -> bool:
        # TODO: Do we really need this?
        return self._find_primary(self._data.validators, view_no) == self._data.name

    def _validate(self, msg: Union[ViewChange, ViewChangeAck, NewView], frm: str) -> int:
        # TODO: Proper validation

        if msg.viewNo < self._data.view_no:
            return DISCARD

        if msg.viewNo == self._data.view_no and not self._data.waiting_for_new_view:
            return DISCARD

        if msg.viewNo > self._data.view_no:
            return STASH

        return PROCESS

    def _send_new_view_if_needed(self):
        if not self._votes.has_view_change_quorum:
            return

        nv = NewView(
            viewNo=self._data.view_no,
            viewChanges=self._votes.confirmed_votes,
            checkpoint=None,
            preprepares=[]
        )
        self._network.send(nv)
        self._data.waiting_for_new_view = False
