from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.internal_messages import ViewChangeInProgress, LedgerSyncStatus, ParticipatingStatus
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.common.stashing_router import StashingRouter
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider
from plenum.server.replica_validator_enums import INCORRECT_INSTANCE, DISCARD, INCORRECT_PP_SEQ_NO, ALREADY_ORDERED, \
    STASH_VIEW, FUTURE_VIEW, OLD_VIEW, PROCESS, GREATER_PREP_CERT, STASH_CATCH_UP, CATCHING_UP, OUTSIDE_WATERMARKS, \
    STASH_WATERMARKS


class OrderingService:
    def __init__(self, data: ConsensusDataProvider, bus: InternalBus, network: ExternalBus):
        self._data = data
        self._bus = bus
        self._network = network
        self._stasher = StashingRouter()

        self._stasher.subscribe(PrePrepare, self.process_preprepare)
        self._stasher.subscribe(Prepare, self.process_prepare)
        self._stasher.subscribe(Commit, self.process_commit)
        self._stasher.subscribe_to(network)

        # ToDo: need to define how to get current value (when object instantiating) for next statuses vars
        self._vc_in_progress = False
        self._is_participating = False
        self._is_synced = False

        self._bus.subscribe(ViewChangeInProgress, self.set_vc_in_progress)
        self._bus.subscribe(LedgerSyncStatus, self.set_is_synced)
        self._bus.subscribe(ParticipatingStatus, self.set_is_participating)

    def process_preprepare(self, pre_prepare: PrePrepare, sender: str):
        result, reason = self._validate(pre_prepare)
        if result == PROCESS:
            self._process_preprepare(pre_prepare, sender)
        return result

    def process_prepare(self, prepare: Prepare, sender: str):
        result, reason = self._validate(prepare)
        if result == PROCESS:
            self._process_prepare(prepare, sender)
        return result

    def process_commit(self, commit: Commit, sender: str):
        result, reason = self._validate(commit)
        if result == PROCESS:
            self._process_commit(commit, sender)
        return result

    def _process_preprepare(self, pre_prepare: PrePrepare, sender: str):
        pass

    def _process_prepare(self, prepare: Prepare, sender: str):
        pass

    def _process_commit(self, commit: Commit, sender: str):
        pass

    @property
    def view_no(self):
        return self._data.view_no

    @property
    def legacy_last_prepared_sertificate(self):
        """
        We assume, that prepared list is an ordered list, and the last element is
        the last quorumed Prepared
        """
        if self._data.prepared:
            last_prepared = self._data.prepared[-1]
            return last_prepared.view_no, last_prepared.pp_seq_no
        return self.last_ordered_3pc

    @property
    def vc_in_progress(self):
        return self._vc_in_progress

    @property
    def is_synced(self):
        return self._is_synced

    @property
    def is_participating(self):
        return self._is_participating

    @vc_in_progress.setter
    def vc_in_progress(self, vc_status: bool):
        self._vc_in_progress = vc_status

    @is_synced.setter
    def is_synced(self, sync_status: bool):
        self._is_synced = sync_status

    @is_participating.setter
    def is_participating(self, particip_status: bool):
        self._is_participating = particip_status

    @property
    def h(self):
        return self._data.h

    @property
    def H(self):
        return self._data.H

    @property
    def last_ordered_3pc(self):
        return self._data.view_no, self._data.pp_seq_no

    def set_vc_in_progress(self, msg: ViewChangeInProgress):
        self.vc_in_progress = msg.in_progress

    def set_is_synced(self, msg: LedgerSyncStatus):
        self.is_synced = msg.is_synced

    def set_is_participating(self, msg: ParticipatingStatus):
        self.is_participating = msg.is_participating

    def has_already_ordered(self, view_no, pp_seq_no):
        return compare_3PC_keys((view_no, pp_seq_no),
                                self.last_ordered_3pc) >= 0

    def _validate(self, msg):
        inst_id = getattr(msg, f.INST_ID.nm, None)
        view_no = getattr(msg, f.VIEW_NO.nm, None)
        pp_seq_no = getattr(msg, f.PP_SEQ_NO.nm, None)

        # ToDO: this checks should be performed in previous level (ReplicaService)
        # 1. Check INSTANCE_ID
        # if inst_id is None or inst_id != self.replica.instId:
        #     return DISCARD, INCORRECT_INSTANCE

        # 2. Check pp_seq_no
        if pp_seq_no == 0:
            # should start with 1
            return DISCARD, INCORRECT_PP_SEQ_NO

        # 3. Check already ordered
        if self.has_already_ordered(view_no, pp_seq_no):
            return DISCARD, ALREADY_ORDERED

        # 4. Check viewNo
        if view_no > self.view_no:
            return STASH_VIEW, FUTURE_VIEW
        if view_no < self.view_no - 1:
            return DISCARD, OLD_VIEW
        if view_no == self.view_no - 1:
            if not isinstance(msg, Commit):
                return DISCARD, OLD_VIEW
            if not self.vc_in_progress:
                return DISCARD, OLD_VIEW
            if self.legacy_last_prepared_sertificate is None:
                return DISCARD, OLD_VIEW
            if compare_3PC_keys((view_no, pp_seq_no), self.legacy_last_prepared_sertificate) < 0:
                return DISCARD, GREATER_PREP_CERT
        if view_no == self.view_no and self.vc_in_progress:
            return STASH_VIEW, FUTURE_VIEW

        # If Catchup in View Change finished then process Commit messages
        if self.is_synced and self.vc_in_progress:
            return PROCESS, None

        # 5. Check if Participating
        if not self.is_participating:
            return STASH_CATCH_UP, CATCHING_UP

        # 6. Check watermarks
        if not (self.h < pp_seq_no <= self.H):
            return STASH_WATERMARKS, OUTSIDE_WATERMARKS

        return PROCESS, None
