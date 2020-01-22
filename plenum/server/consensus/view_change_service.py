import copy
from operator import itemgetter
from typing import List, Optional, Union, Dict, Any

from plenum.common.config_util import getConfig
from plenum.common.constants import VIEW_CHANGE, PRIMARY_SELECTION_PREFIX, NEW_VIEW
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.internal_messages import NeedViewChange, NewViewAccepted, ViewChangeStarted, MissingMessage, \
    VoteForViewChange, NodeNeedViewChange
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck, NewView, Checkpoint
from plenum.common.router import Subscription
from plenum.common.stashing_router import StashingRouter, DISCARD, PROCESS
from plenum.common.timer import TimerService, RepeatingTimer
from plenum.common.types import f
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.batch_id import BatchID
from plenum.server.consensus.primary_selector import PrimariesSelector
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.server.consensus.view_change_storages import view_change_digest
from plenum.server.replica_helper import generateName
from plenum.server.replica_validator_enums import STASH_WAITING_VIEW_CHANGE, STASH_CATCH_UP
from plenum.server.suspicion_codes import Suspicions, Suspicion
from stp_core.common.log import getlogger

logger = getlogger()


class ViewChangeService:
    def __init__(self, data: ConsensusSharedData, timer: TimerService, bus: InternalBus, network: ExternalBus,
                 stasher: StashingRouter, primaries_selector: PrimariesSelector):
        self._config = getConfig()

        self._data = data
        self._new_view_builder = NewViewBuilder(self._data)
        self._timer = timer
        self._bus = bus
        self._network = network
        self._router = stasher

        # Last successful viewNo.
        # In some cases view_change process can be uncompleted in time.
        # In that case we want to know, which viewNo was successful (last completed view_change)
        self.last_completed_view_no = self._data.view_no

        self._resend_inst_change_timer = RepeatingTimer(self._timer,
                                                        self._config.NEW_VIEW_TIMEOUT,
                                                        self._propose_view_change_not_complete_in_time,
                                                        active=False)

        self._old_prepared = {}  # type: Dict[int, BatchID]
        self._old_preprepared = {}  # type: Dict[int, List[BatchID]]
        self._stashed_vc_msgs = {}  # type: Dict[int, int]
        self._primaries_selector = primaries_selector

        self._subscription = Subscription()
        self._subscription.subscribe(self._router, ViewChange, self.process_view_change_message)
        self._subscription.subscribe(self._router, ViewChangeAck, self.process_view_change_ack_message)
        self._subscription.subscribe(self._router, NewView, self.process_new_view_message)
        self._subscription.subscribe(self._bus, NeedViewChange, self.process_need_view_change)

    def cleanup(self):
        self._subscription.unsubscribe_all()

    def __repr__(self):
        return self._data.name

    @property
    def view_change_votes(self):
        return self._data.view_change_votes

    def process_need_view_change(self, msg: NeedViewChange):
        logger.info("{} processing {}".format(self, msg))

        # 1. calculate new viewno
        view_no = msg.view_no
        if view_no is None:
            view_no = self._data.view_no + 1

        # 2. Do cleanup before new view change starts
        self._clean_on_view_change_start()

        # 3. Update shared data
        self._data.view_no = view_no
        self._data.waiting_for_new_view = True
        old_primary = self._data.primary_name
        self._data.primary_name = None
        if not self._data.is_master:
            self._data.master_reordered_after_vc = False
            return

        # Only the master primary is selected at the beginning of view change as we need to get a NEW_VIEW and do re-ordering on master
        # Backup primaries will not be selected (and backups will not order) until re-ordering of txns from previous view on master is finished
        # More precisely, it will be done after the first batch in a new view is committed
        # This is done so as N and F may change as a result of NODE txns ordered in last view,
        # so we need a synchronous point of updating N, F, number of replicas and backup primaris
        # Beginning of view (when the first batch in a view is ordered) is such a point.
        self._data.primary_name = generateName(self._primaries_selector.select_master_primary(self._data.view_no),
                                               self._data.inst_id)

        if old_primary and self._data.primary_name == old_primary:
            logger.info("Selected master primary is the same with the "
                        "current master primary (new_view {}). "
                        "Propose a new view {}".format(self._data.view_no,
                                                       self._data.view_no + 1))
            self._propose_view_change(Suspicions.INCORRECT_NEW_PRIMARY)

        logger.info("{} started view change to view {}. Expected Master Primary: {}".format(self._data.name,
                                                                                            self._data.view_no,
                                                                                            self._data.primary_name))

        # 4. Build ViewChange message
        vc = self._build_view_change_msg()

        # 5. Send ViewChangeStarted via internal bus to update other services
        logger.info("{} sending {}".format(self, vc))
        self._bus.send(ViewChangeStarted(view_no=self._data.view_no))

        # 6. Send ViewChange msg to other nodes (via external bus)
        self._network.send(vc)
        self.view_change_votes.add_view_change(vc, self._data.name)

        # 7. Unstash messages for view change
        self._router.process_all_stashed(STASH_WAITING_VIEW_CHANGE)
        self._stashed_vc_msgs.clear()

        # 8. Restart instance change timer
        self._resend_inst_change_timer.stop()
        self._resend_inst_change_timer.start()

    def _clean_on_view_change_start(self):
        self._clear_old_batches(self._old_prepared)
        self._clear_old_batches(self._old_preprepared)
        self.view_change_votes.clear()
        self._data.new_view_votes.clear()

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
        result, reason = self._validate(msg, frm)
        if result == STASH_WAITING_VIEW_CHANGE:
            self._stashed_vc_msgs.setdefault(msg.viewNo, 0)
            self._stashed_vc_msgs[msg.viewNo] += 1
            if self._data.quorums.view_change.is_reached(self._stashed_vc_msgs[msg.viewNo]) and \
                    not self._data.waiting_for_new_view:
                self._bus.send(NodeNeedViewChange(msg.viewNo))
        if result != PROCESS:
            return result, reason

        logger.info("{} processing {} from {}".format(self, msg, frm))

        self.view_change_votes.add_view_change(msg, frm)

        vca = ViewChangeAck(
            viewNo=msg.viewNo,
            name=replica_name_to_node_name(frm),
            digest=view_change_digest(msg)
        )
        self.view_change_votes.add_view_change_ack(vca, replica_name_to_node_name(self._data.name))

        if self._data.is_primary:
            self._send_new_view_if_needed()
            return PROCESS, None

        primary_node_name = replica_name_to_node_name(self._data.primary_name)
        logger.info("{} sending {}".format(self, vca))
        self._network.send(vca, [primary_node_name])

        self._finish_view_change_if_needed()
        return PROCESS, None

    def process_view_change_ack_message(self, msg: ViewChangeAck, frm: str):
        result, reason = self._validate(msg, frm)
        if result != PROCESS:
            return result, reason

        logger.info("{} processing {} from {}".format(self, msg, frm))

        if not self._data.is_primary:
            return PROCESS, None

        self.view_change_votes.add_view_change_ack(msg, frm)
        self._send_new_view_if_needed()
        return PROCESS, None

    def process_new_view_message(self, msg: NewView, frm: str):
        result, reason = self._validate(msg, frm)
        if result != PROCESS:
            return result, reason

        logger.info("{} processing {} from {}".format(self, msg, frm))
        if frm != self._data.primary_name:
            logger.warning("Received NEW_VIEW message from non-primary node. "
                           "Selected primaries may differ on other nodes")

        self._data.new_view_votes.add_new_view(msg, frm)
        self._finish_view_change_if_needed()
        return PROCESS, None

    def _validate(self, msg: Union[ViewChange, ViewChangeAck, NewView], frm: str) -> (int, str):
        # TODO: Proper validation
        if not self._data.is_master:
            return DISCARD, "not master instance"

        if msg.viewNo < self._data.view_no:
            return DISCARD, "message has old view {}, current view {}".format(msg.viewNo, self._data.view_no)

        if msg.viewNo == self._data.view_no and not self._data.waiting_for_new_view:
            return DISCARD, "message has current view {}, but we already finished view change".format(msg.viewNo)

        if not self._data.is_participating:
            return STASH_CATCH_UP, "we're not participating yet"

        if msg.viewNo > self._data.view_no:
            return STASH_WAITING_VIEW_CHANGE, "message is from future view {}".format(msg.viewNo)

        return PROCESS, None

    def _send_new_view_if_needed(self):
        confirmed_votes = self.view_change_votes.confirmed_votes
        if not self._data.quorums.view_change.is_reached(len(confirmed_votes)):
            logger.debug("{} can not send NEW_VIEW: no quorum. Has {}, expects {} votes".format(
                self._data.name, len(confirmed_votes), self._data.quorums.view_change.value))
            return

        view_changes = [self.view_change_votes.get_view_change(*v) for v in confirmed_votes]
        cp = self._new_view_builder.calc_checkpoint(view_changes)
        if cp is None:
            logger.info("{} can not send NEW_VIEW: can not calculate Checkpoint".format(self._data.name))
            return

        batches = self._new_view_builder.calc_batches(cp, view_changes)
        if batches is None:
            logger.info("{} can not send NEW_VIEW: can not calculate Batches".format(self._data.name))
            return

        if cp not in self._data.checkpoints:
            logger.info("{} can not send NEW_VIEW: does not have Checkpoint {}.".format(self._data.name, str(cp)))
            return

        nv = NewView(
            viewNo=self._data.view_no,
            viewChanges=sorted(confirmed_votes, key=itemgetter(0)),
            checkpoint=cp,
            batches=batches
        )
        logger.info("{} sending {}".format(self, nv))
        self._network.send(nv)
        self._data.new_view_votes.add_new_view(nv, self._data.name)
        self._finish_view_change()

    def _finish_view_change_if_needed(self):
        if self._data.new_view is None:
            return

        view_changes = []
        for name, vc_digest in self._data.new_view.viewChanges:
            vc = self.view_change_votes.get_view_change(name, vc_digest)
            # We don't have needed ViewChange, so we cannot validate NewView
            if vc is None:
                self._request_view_change_message((name, vc_digest))
                return
            view_changes.append(vc)

        cp = self._new_view_builder.calc_checkpoint(view_changes)
        if cp is None or cp != self._data.new_view.checkpoint:
            # New primary is malicious
            logger.info(
                "{} Received invalid NewView {} for view {}: expected checkpoint {}".format(self._data.name,
                                                                                            self._data.new_view,
                                                                                            self._data.view_no,
                                                                                            cp)
            )
            self._propose_view_change(Suspicions.NEW_VIEW_INVALID_CHECKPOINTS)
            return

        batches = self._new_view_builder.calc_batches(cp, view_changes)
        if batches != self._data.new_view.batches:
            # New primary is malicious
            logger.info(
                "{} Received invalid NewView {} for view {}: expected batches {}".format(self._data.name,
                                                                                         self._data.new_view,
                                                                                         self._data.view_no,
                                                                                         batches)
            )
            self._propose_view_change(Suspicions.NEW_VIEW_INVALID_BATCHES)
            return

        self._finish_view_change()

    def _finish_view_change(self):
        # Update shared data
        self._data.waiting_for_new_view = False
        self._data.prev_view_prepare_cert = self._data.new_view.batches[-1].pp_seq_no \
            if self._data.new_view.batches else self._data.new_view.checkpoint.seqNoEnd
        if f.PRIMARY.nm in self._data.new_view:
            self._data.primary_name = generateName(self._data.new_view.primary,
                                                   self._data.inst_id)

        logger.info("{} finished view change to view {}. Master Primary: {}".format(self._data.name,
                                                                                    self._data.view_no,
                                                                                    self._data.primary_name))
        # Cancel View Change timeout task
        self._resend_inst_change_timer.stop()
        # send message to other services
        self._bus.send(NewViewAccepted(view_no=self._data.new_view.viewNo,
                                       view_changes=self._data.new_view.viewChanges,
                                       checkpoint=self._data.new_view.checkpoint,
                                       batches=self._data.new_view.batches))
        self.last_completed_view_no = self._data.view_no

    def _propose_view_change_not_complete_in_time(self):
        self._propose_view_change(Suspicions.INSTANCE_CHANGE_TIMEOUT)
        if self._data.new_view is None:
            self._request_new_view_message(self._data.view_no)

    def _propose_view_change(self, suspicion: Suspicion):
        self._bus.send(VoteForViewChange(suspicion))

    def _request_new_view_message(self, view_no):
        self._bus.send(MissingMessage(msg_type=NEW_VIEW,
                                      key=view_no,
                                      inst_id=self._data.inst_id,
                                      dst=None,
                                      stash_data=None))

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
