from _sha256 import sha256
from collections import defaultdict
from functools import partial
from typing import List, Optional, Union, NamedTuple, Dict, Any, Tuple

from common.serializers.json_serializer import JsonSerializer
from plenum.common.config_util import getConfig
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck, NewView, PrePrepare, Checkpoint
from plenum.common.stashing_router import StashingRouter, PROCESS, DISCARD, STASH
from plenum.common.timer import TimerService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.quorums import Quorums
from stp_core.common.log import getlogger

BatchID = NamedTuple('BatchID', [('view_no', int), ('pp_seq_no', int), ('pp_digest', str)])


def view_change_digest(msg: ViewChange) -> str:
    msg_as_dict = msg.__dict__
    msg_as_dict['checkpoints'] = [cp.__dict__ for cp in msg_as_dict['checkpoints']]
    serialized = JsonSerializer().dumps(msg_as_dict)
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
    def digest(self) -> Optional[str]:
        """
        Returns digest of received view change message
        """
        return self._digest

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
    def confirmed_votes(self) -> List[Tuple[str, str]]:
        return [(frm, node_votes.digest) for frm, node_votes in self._votes.items()
                if node_votes.is_confirmed]

    def get_view_change(self, frm: str, digest: str) -> Optional[ViewChange]:
        vc = self._votes[frm].view_change
        if vc is not None and view_change_digest(vc) == digest:
            return vc

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
    def __init__(self, data: ConsensusSharedData, timer: TimerService, bus: InternalBus, network: ExternalBus):
        self._config = getConfig()
        self._logger = getlogger()

        self._data = data
        self._new_view_builder = NewViewBuilder(self._data)
        self._timer = timer
        self._bus = bus
        self._network = network
        self._router = StashingRouter(self._config.VIEW_CHANGE_SERVICE_STASH_LIMIT)
        self._votes = ViewChangeVotesForView(self._data.quorums)
        self._new_view = None   # type: Optional[NewView]

        self._router.subscribe(ViewChange, self.process_view_change_message)
        self._router.subscribe(ViewChangeAck, self.process_view_change_ack_message)
        self._router.subscribe(NewView, self.process_new_view_message)
        self._router.subscribe_to(network)

        self._old_prepared = {}     # type: Dict[int, BatchID]
        self._old_preprepared = {}  # type: Dict[int, List[BatchID]]

    def __repr__(self):
        return self._data.name

    def start_view_change(self, view_no: Optional[int] = None):
        if view_no is None:
            view_no = self._data.view_no + 1

        self._clear_old_batches(self._old_prepared)
        self._clear_old_batches(self._old_preprepared)

        for pp in self._data.prepared:
            self._old_prepared[pp.ppSeqNo] = self.batch_id(pp)
        prepared = sorted([tuple(bid) for bid in self._old_prepared.values()])

        for pp in self._data.preprepared:
            new_bid = self.batch_id(pp)
            pretenders = self._old_preprepared.get(pp.ppSeqNo, [])
            pretenders = [bid for bid in pretenders
                          if bid.pp_digest != new_bid.pp_digest]
            pretenders.append(new_bid)
            self._old_preprepared[pp.ppSeqNo] = pretenders
        preprepared = sorted([tuple(bid) for bids in self._old_preprepared.values() for bid in bids])

        self._data.view_no = view_no
        self._data.waiting_for_new_view = True
        self._data.primary_name = self._find_primary(self._data.validators, self._data.view_no)
        self._data.preprepared.clear()
        self._data.prepared.clear()
        self._votes.clear()
        self._new_view = None

        vc = ViewChange(
            viewNo=self._data.view_no,
            stableCheckpoint=self._data.stable_checkpoint,
            prepared=prepared,
            preprepared=preprepared,
            checkpoints=list(self._data.checkpoints)
        )
        self._network.send(vc)
        self._votes.add_view_change(vc, self._data.name)

        self._router.process_all_stashed()

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

        self._finish_view_change_if_needed()

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

        self._new_view = msg

        self._finish_view_change_if_needed()

    @staticmethod
    def _find_primary(validators: List[str], view_no: int) -> str:
        return validators[view_no % len(validators)]

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
        confirmed_votes = self._votes.confirmed_votes
        if not self._data.quorums.view_change.is_reached(len(confirmed_votes)):
            return

        view_changes = [self._votes.get_view_change(*v) for v in confirmed_votes]
        cp = self._new_view_builder.calc_checkpoint(view_changes)
        if cp is None:
            return

        batches = self._new_view_builder.calc_batches(cp, view_changes)
        if batches is None:
            return

        nv = NewView(
            viewNo=self._data.view_no,
            viewChanges=confirmed_votes,
            checkpoint=cp,
            batches=batches
        )
        self._network.send(nv)
        self._new_view = nv
        self._finish_view_change(cp, batches)

    def _finish_view_change_if_needed(self):
        if self._new_view is None:
            return

        view_changes = []
        for name, vc_digest in self._new_view.viewChanges:
            vc = self._votes.get_view_change(name, vc_digest)
            # We don't have needed ViewChange, so we cannot validate NewView
            if vc is None:
                return
            view_changes.append(vc)

        cp = self._new_view_builder.calc_checkpoint(view_changes)
        if cp is None or cp != self._new_view.checkpoint:
            # New primary is malicious
            self.start_view_change()
            assert False  # TODO: Test debugging purpose
            return

        batches = self._new_view_builder.calc_batches(cp, view_changes)
        if batches != self._new_view.batches:
            # New primary is malicious
            self.start_view_change()
            assert False  # TODO: Test debugging purpose
            return

        self._finish_view_change(cp, batches)

    def _finish_view_change(self, cp: Checkpoint, batches: List[BatchID]):
        # Update checkpoint
        # TODO: change to self._bus.send(FinishViewChange(cp)) in scope of the task INDY-2179
        self._data.stable_checkpoint = cp.seqNoEnd
        self._data.checkpoints = [old_cp for old_cp in self._data.checkpoints if old_cp.seqNoEnd > cp.seqNoEnd]
        self._data.checkpoints.append(cp)

        # Update batches
        # TODO: Actually we'll need to retrieve preprepares by ID from somewhere
        self._data.preprepared = batches

        # We finished a view change!
        self._data.waiting_for_new_view = False

    def _clear_old_batches(self, batches: Dict[int, Any]):
        for pp_seq_no in list(batches.keys()):
            if pp_seq_no <= self._data.stable_checkpoint:
                del batches[pp_seq_no]

    @staticmethod
    def batch_id(batch: PrePrepare):
        return BatchID(batch.viewNo, batch.ppSeqNo, batch.digest)


class NewViewBuilder:

    def __init__(self, data: ConsensusSharedData) -> None:
        self._data = data

    def calc_checkpoint(self, vcs: List[ViewChange]) -> Optional[Checkpoint]:
        checkpoints = []
        for cur_vc in vcs:
            for cur_cp in cur_vc.checkpoints:
                # Don't add checkpoint to pretending ones if it is already there
                if cur_cp in checkpoints:
                    continue

                # Don't add checkpoint to pretending ones if too many nodes already stabilized it
                # TODO: Should we take into account view_no as well?
                stable_checkpoint_not_higher = [vc for vc in vcs if cur_cp.seqNoEnd >= vc.stableCheckpoint]
                if not self._data.quorums.strong.is_reached(len(stable_checkpoint_not_higher)):
                    continue

                # Don't add checkpoint to pretending ones if not enough nodes have it
                have_checkpoint = [vc for vc in vcs if cur_cp in vc.checkpoints]
                if not self._data.quorums.weak.is_reached(len(have_checkpoint)):
                    continue

                # All checks passed, this is a valid candidate checkpoint
                checkpoints.append(cur_cp)

        highest_cp = None
        for cp in checkpoints:
            # TODO: Should we take into account view_no as well?
            if highest_cp is None or cp.seqNoEnd > highest_cp.seqNoEnd:
                highest_cp = cp

        return highest_cp

    def calc_batches(self, cp: Checkpoint, vcs: List[ViewChange]) -> Optional[List[BatchID]]:
        # TODO: Optimize this
        batches = set()
        pp_seq_no = cp.seqNoEnd + 1
        while pp_seq_no <= cp.seqNoEnd + self._data.log_size:
            bid = self._try_find_batch_for_pp_seq_no(vcs, pp_seq_no)
            if bid:
                batches.add(bid)
                pp_seq_no += 1
                continue

            if self._check_null_batch(vcs, pp_seq_no):
                # TODO: the protocol says to do the loop for all pp_seq_no till h+L (apply NULL batches)
                # Since we require sequential applying of PrePrepares, we can stop on the first non-found (NULL) batch
                # Double-check this!
                break

            # not enough quorums yet
            return None

        return sorted(batches)

    def _try_find_batch_for_pp_seq_no(self, vcs, pp_seq_no):
        for vc in vcs:
            for _bid in vc.prepared:
                bid = BatchID(*_bid)
                if bid.pp_seq_no != pp_seq_no:
                    continue
                if not self._is_batch_prepared(bid, vcs):
                    continue
                if not self._is_batch_preprepared(bid, vcs):
                    continue
                return bid

        return None

    def _is_batch_prepared(self, bid: BatchID, vcs: List[ViewChange]) -> bool:
        def check(vc: ViewChange):
            if bid.pp_seq_no <= vc.stableCheckpoint:
                return False

            for _some_bid in vc.prepared:
                some_bid = BatchID(*_some_bid)
                if some_bid.pp_seq_no != bid.pp_seq_no:
                    continue
                # not ( (v' < v) OR (v'==v and d'==d) )
                if some_bid.view_no > bid.view_no:
                    return False
                if some_bid.view_no >= bid.view_no and some_bid.pp_digest != bid.pp_digest:
                    return False
            return True

        prepared_witnesses = sum(1 for vc in vcs if check(vc))
        return self._data.quorums.strong.is_reached(prepared_witnesses)

    def _is_batch_preprepared(self, bid: BatchID, vcs: List[ViewChange]) -> bool:
        def check(vc: ViewChange):
            for _some_bid in vc.preprepared:
                some_bid = BatchID(*_some_bid)
                if some_bid.pp_seq_no != bid.pp_seq_no:
                    continue
                if some_bid.pp_digest != bid.pp_digest:
                    continue
                if some_bid.view_no >= bid.view_no:
                    return True

            return False

        preprepared_witnesses = sum(1 for vc in vcs if check(vc))
        return self._data.quorums.weak.is_reached(preprepared_witnesses)

    def _check_null_batch(self, vcs, pp_seq_no):
        def check(vc: ViewChange):
            if pp_seq_no <= vc.stableCheckpoint:
                return False

            for _some_bid in vc.prepared:
                some_bid = BatchID(*_some_bid)
                if some_bid.pp_seq_no == pp_seq_no:
                    return False
            return True

        null_batch_witnesses = sum(1 for vc in vcs if check(vc))
        return self._data.quorums.strong.is_reached(null_batch_witnesses)
