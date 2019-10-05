from _sha256 import sha256
from collections import defaultdict
from functools import partial
from operator import itemgetter
from typing import List, Optional, Union, Dict, Any, Tuple

from common.serializers.json_serializer import JsonSerializer
from plenum.common.config_util import getConfig
from plenum.common.constants import VIEW_CHANGE
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.internal_messages import NeedViewChange, NewViewAccepted, ViewChangeStarted, MissingMessage
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck, NewView, Checkpoint, InstanceChange
from plenum.common.router import Subscription
from plenum.common.stashing_router import StashingRouter, DISCARD, PROCESS
from plenum.common.timer import TimerService, RepeatingTimer
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.batch_id import BatchID
from plenum.server.consensus.primary_selector import RoundRobinPrimariesSelector
from plenum.server.consensus.view_change_storages import view_change_digest
from plenum.server.replica_helper import generateName, getNodeName
from plenum.server.replica_validator_enums import STASH_VIEW_3PC, STASH_WAITING_VIEW_CHANGE
from plenum.server.suspicion_codes import Suspicions
from stp_core.common.log import getlogger


class ViewChangeService:
    def __init__(self, data: ConsensusSharedData, timer: TimerService, bus: InternalBus, network: ExternalBus,
                 stasher: StashingRouter):
        self._config = getConfig()
        self._logger = getlogger()

        self._data = data
        self._new_view_builder = NewViewBuilder(self._data)
        self._timer = timer
        self._bus = bus
        self._network = network
        self._router = stasher
        self._new_view = None  # type: Optional[NewView]
        self._resend_inst_change_timer = RepeatingTimer(self._timer,
                                                        self._config.NEW_VIEW_TIMEOUT,
                                                        partial(self._propose_view_change,
                                                                Suspicions.INSTANCE_CHANGE_TIMEOUT.code),
                                                        active=False)

        self._router.subscribe(ViewChange, self.process_view_change_message)
        self._router.subscribe(ViewChangeAck, self.process_view_change_ack_message)
        self._router.subscribe(NewView, self.process_new_view_message)

        self._old_prepared = {}  # type: Dict[int, BatchID]
        self._old_preprepared = {}  # type: Dict[int, List[BatchID]]
        self._primaries_selector = RoundRobinPrimariesSelector()

        self._subscription = Subscription()
        self._subscription.subscribe(self._bus, NeedViewChange, self.process_need_view_change)

    def __repr__(self):
        return self._data.name

    @property
    def view_change_votes(self):
        return self._data.view_change_votes

    def process_need_view_change(self, msg: NeedViewChange):
        self._logger.info("{} processing {}".format(self, msg))

        # 1. calculate new viewno
        view_no = msg.view_no
        if view_no is None:
            view_no = self._data.view_no + 1

        # 2. Do cleanup before new view change starts
        self._clean_on_view_change_start()

        # 3. Update shared data
        self._data.view_no = view_no
        self._data.waiting_for_new_view = True
        self._data.primaries = self._primaries_selector.select_primaries(view_no=self._data.view_no,
                                                                         instance_count=self._data.quorums.f + 1,
                                                                         validators=self._data.validators)
        self._data.primary_name = generateName(self._data.primaries[self._data.inst_id], self._data.inst_id)

        if not self._data.is_master:
            self._data.waiting_for_new_view = False
            self._bus.send(NewViewAccepted(view_no=view_no,
                                           view_changes=None,
                                           checkpoint=None,
                                           batches=None))
            self._router.process_all_stashed(STASH_VIEW_3PC)
            return

        # 4. Build ViewChange message
        vc = self._build_view_change_msg()

        # 5. Send ViewChangeStarted via internal bus to update other services
        self._logger.info("{} sending {}".format(self, vc))
        self._bus.send(ViewChangeStarted(view_no=self._data.view_no))

        # 6. Send ViewChange msg to other nodes (via external bus)
        self._network.send(vc)
        self.view_change_votes.add_view_change(vc, self._data.name)

        # 7. Unstash messages for view change
        self._router.process_all_stashed(STASH_WAITING_VIEW_CHANGE)

        # 8. Restart instance change timer
        self._resend_inst_change_timer.stop()
        self._resend_inst_change_timer.start()

    def _clean_on_view_change_start(self):
        self._clear_old_batches(self._old_prepared)
        self._clear_old_batches(self._old_preprepared)
        self.view_change_votes.clear()
        self._new_view = None

    def _clear_old_batches(self, batches: Dict[int, Any]):
        for pp_seq_no in list(batches.keys()):
            if pp_seq_no <= self._data.stable_checkpoint:
                del batches[pp_seq_no]

    def _build_view_change_msg(self):
        for batch_id in self._data.prepared:
            self._old_prepared[batch_id.pp_seq_no] = batch_id
        prepared = sorted(list(self._old_prepared.values()))

        for new_bid in self._data.preprepared:
            pretenders = self._old_preprepared.get(new_bid.pp_seq_no, [])
            pretenders = [bid for bid in pretenders
                          if bid.pp_digest != new_bid.pp_digest]
            pretenders.append(new_bid)
            self._old_preprepared[new_bid.pp_seq_no] = pretenders
        preprepared = sorted([bid for bids in self._old_preprepared.values() for bid in bids])

        return ViewChange(
            viewNo=self._data.view_no,
            stableCheckpoint=self._data.stable_checkpoint,
            prepared=prepared,
            preprepared=preprepared,
            checkpoints=list(self._data.checkpoints)
        )

    def process_view_change_message(self, msg: ViewChange, frm: str):
        result = self._validate(msg, frm)
        if result != PROCESS:
            return result, None

        self._logger.info("{} processing {} from {}".format(self, msg, frm))

        self.view_change_votes.add_view_change(msg, frm)

        vca = ViewChangeAck(
            viewNo=msg.viewNo,
            name=getNodeName(frm),
            digest=view_change_digest(msg)
        )
        self.view_change_votes.add_view_change_ack(vca, getNodeName(self._data.name))

        if self._data.is_primary:
            self._send_new_view_if_needed()
            return PROCESS, None

        primary_node_name = getNodeName(self._data.primary_name)
        self._logger.info("{} sending {}".format(self, vca))
        self._network.send(vca, [primary_node_name])

        self._finish_view_change_if_needed()
        return PROCESS, None

    def process_view_change_ack_message(self, msg: ViewChangeAck, frm: str):
        result = self._validate(msg, frm)
        if result != PROCESS:
            return result, None

        self._logger.info("{} processing {} from {}".format(self, msg, frm))

        if not self._data.is_primary:
            return PROCESS, None

        self.view_change_votes.add_view_change_ack(msg, frm)
        self._send_new_view_if_needed()
        return PROCESS, None

    def process_new_view_message(self, msg: NewView, frm: str):
        result = self._validate(msg, frm)
        if result != PROCESS:
            return result, None

        self._logger.info("{} processing {} from {}".format(self, msg, frm))

        if frm != self._data.primary_name:
            self._logger.info(
                "{} Received NewView {} for view {} from non-primary {}; expected primary {}".format(self._data.name,
                                                                                                     msg,
                                                                                                     self._data.view_no,
                                                                                                     frm,
                                                                                                     self._data.primary_name)
            )
            return DISCARD, "New View from non-Primary"

        self._new_view = msg
        self._finish_view_change_if_needed()
        return PROCESS, None

    def _validate(self, msg: Union[ViewChange, ViewChangeAck, NewView], frm: str) -> int:
        # TODO: Proper validation
        if not self._data.is_master:
            return DISCARD

        if msg.viewNo < self._data.view_no:
            return DISCARD

        if msg.viewNo == self._data.view_no and not self._data.waiting_for_new_view:
            return DISCARD

        if msg.viewNo > self._data.view_no:
            return STASH_WAITING_VIEW_CHANGE

        return PROCESS

    def _send_new_view_if_needed(self):
        confirmed_votes = self.view_change_votes.confirmed_votes
        if not self._data.quorums.view_change.is_reached(len(confirmed_votes)):
            return

        view_changes = [self.view_change_votes.get_view_change(*v) for v in confirmed_votes]
        cp = self._new_view_builder.calc_checkpoint(view_changes)
        if cp is None:
            return

        batches = self._new_view_builder.calc_batches(cp, view_changes)
        if batches is None:
            return

        nv = NewView(
            viewNo=self._data.view_no,
            viewChanges=sorted(confirmed_votes, key=itemgetter(0)),
            checkpoint=cp,
            batches=batches
        )
        self._logger.info("{} sending {}".format(self, nv))
        self._network.send(nv)
        self._new_view = nv
        self._finish_view_change()

    def _finish_view_change_if_needed(self):
        if self._new_view is None:
            return

        view_changes = []
        for name, vc_digest in self._new_view.viewChanges:
            vc = self.view_change_votes.get_view_change(name, vc_digest)
            # We don't have needed ViewChange, so we cannot validate NewView
            if vc is None:
                self._request_view_change_message((name, vc_digest))
                return
            view_changes.append(vc)

        cp = self._new_view_builder.calc_checkpoint(view_changes)
        if cp is None or cp != self._new_view.checkpoint:
            # New primary is malicious
            self._logger.info(
                "{} Received invalid NewView {} for view {}: expected checkpoint {}".format(self._data.name,
                                                                                            self._new_view,
                                                                                            self._data.view_no,
                                                                                            cp)
            )
            self._propose_view_change(Suspicions.NEW_VIEW_INVALID_CHECKPOINTS.code)
            return

        batches = self._new_view_builder.calc_batches(cp, view_changes)
        if batches != self._new_view.batches:
            # New primary is malicious
            self._logger.info(
                "{} Received invalid NewView {} for view {}: expected batches {}".format(self._data.name,
                                                                                         self._new_view,
                                                                                         self._data.view_no,
                                                                                         batches)
            )
            self._propose_view_change(Suspicions.NEW_VIEW_INVALID_BATCHES.code)
            return

        self._finish_view_change()

    def _finish_view_change(self):
        # Update shared data
        self._data.waiting_for_new_view = False
        self._data.prev_view_prepare_cert = self._new_view.batches[-1].pp_seq_no if self._new_view.batches else 0

        # Cancel View Change timeout task
        self._resend_inst_change_timer.stop()
        # send message to other services
        self._bus.send(NewViewAccepted(view_no=self._new_view.viewNo,
                                       view_changes=self._new_view.viewChanges,
                                       checkpoint=self._new_view.checkpoint,
                                       batches=self._new_view.batches))

    def _propose_view_change(self, suspision_code):
        proposed_view_no = self._data.view_no + 1
        self._logger.info("{} proposing a view change to {} with code {}".
                          format(self, proposed_view_no, suspision_code))
        msg = InstanceChange(proposed_view_no, suspision_code)
        self._network.send(msg)

    def _request_view_change_message(self, key):
        self._bus.send(MissingMessage(msg_type=VIEW_CHANGE,
                                      key=key,
                                      inst_id=self._data.inst_id,
                                      dst=None,
                                      stash_data=None))


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
                # TODO: PBFT paper (for example Fig.4 in https://www.microsoft.com/en-us/research/wp-content/uploads/2017/01/p398-castro-bft-tocs.pdf)
                # assumes a weak certificate here.
                # But they also assume a need of catch-up before processing NewView if a Replica doesn't have the calculated checkpoint yet
                # It looks like using a strong certificate eliminates a need for cathcup (although re-ordering may be slower)
                # Once https://jira.hyperledger.org/browse/INDY-2237 is done, we may come back to weak certificate here
                have_checkpoint = [vc for vc in vcs if cur_cp in vc.checkpoints]
                if not self._data.quorums.strong.is_reached(len(have_checkpoint)):
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

                # not ( (v' < v) OR (v'==v and d'==d and pp_view_no'==pp_view_no) )
                if some_bid.view_no > bid.view_no:
                    return False
                if some_bid.view_no >= bid.view_no and some_bid.pp_digest != bid.pp_digest:
                    return False
                if some_bid.view_no >= bid.view_no and some_bid.pp_view_no != bid.pp_view_no:
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
                if some_bid.pp_view_no != bid.pp_view_no:
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
