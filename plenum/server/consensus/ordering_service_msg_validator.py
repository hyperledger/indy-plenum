from typing import Tuple, Optional

from common.exceptions import LogicError
from plenum.common.messages.internal_messages import NewViewCheckpointsApplied
from plenum.common.messages.node_messages import PrePrepare, Commit, Prepare, OldViewPrePrepareRequest, \
    OldViewPrePrepareReply
from plenum.common.stashing_router import DISCARD, PROCESS
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.replica_validator_enums import STASH_WATERMARKS, STASH_VIEW_3PC, STASH_CATCH_UP, \
    ALREADY_ORDERED, OUTSIDE_WATERMARKS, CATCHING_UP, FUTURE_VIEW, OLD_VIEW, WAITING_FOR_NEW_VIEW, NON_MASTER, \
    INCORRECT_PP_SEQ_NO, INCORRECT_INSTANCE, STASH_WAITING_FIRST_BATCH_IN_VIEW, WAITING_FIRST_BATCH_IN_VIEW


class OrderingServiceMsgValidator:

    def __init__(self, data: ConsensusSharedData):
        self._data = data

    # TODO: this method is created for compatibility adn tests only
    # validate_XXX methods will be used later
    def validate(self, msg) -> Tuple[int, Optional[str]]:
        if isinstance(msg, PrePrepare):
            return self.validate_pre_prepare(msg)
        if isinstance(msg, Prepare):
            return self.validate_prepare(msg)
        if isinstance(msg, Commit):
            return self.validate_commit(msg)
        if isinstance(msg, NewViewCheckpointsApplied):
            return self.validate_new_view(msg)
        if isinstance(msg, OldViewPrePrepareRequest):
            return self.validate_old_view_prep_prepare_req(msg)
        if isinstance(msg, OldViewPrePrepareReply):
            return self.validate_old_view_prep_prepare_rep(msg)
        raise LogicError("Unknown message type")

    def validate_pre_prepare(self, msg: PrePrepare) -> Tuple[int, Optional[str]]:
        # we discard already ordered PrePrepares
        # check for discarded first of all

        # TODO: allow re-order of ordered pre-prepares below last_prep_certificate for a view only
        # view_no = getattr(msg, f.VIEW_NO.nm, None)
        # pp_seq_no = getattr(msg, f.PP_SEQ_NO.nm, None)
        # if self.has_already_ordered(view_no, pp_seq_no):
        #     return DISCARD, ALREADY_ORDERED

        return self._validate_3pc(msg)

    def validate_prepare(self, msg: Prepare) -> Tuple[int, Optional[str]]:
        # process Prepares that have been already ordered
        # to re-order batches from NEW_VIEW msg
        # prepares below low watermark have been discarded anyway
        return self._validate_3pc(msg)

    def validate_commit(self, msg: Commit) -> Tuple[int, Optional[str]]:
        # process Prepares that have been already ordered
        # to re-order batches from NEW_VIEW msg
        # prepares below low watermark have been discarded anyway
        return self._validate_3pc(msg)

    def validate_new_view(self, msg: NewViewCheckpointsApplied) -> Tuple[int, Optional[str]]:
        # View Change service has already validated NewView
        # so basic validation here is sufficient
        return self._validate_base(msg, msg.view_no)

    def validate_old_view_prep_prepare_req(self, msg: OldViewPrePrepareRequest):
        inst_id = getattr(msg, f.INST_ID.nm, None)

        # Check INSTANCE_ID
        if inst_id is None or inst_id != self._data.inst_id:
            return DISCARD, INCORRECT_INSTANCE

        if not self._data.is_master:
            return DISCARD, NON_MASTER

        # Check if catchup is in progress
        if not self._data.is_participating:
            return STASH_CATCH_UP, CATCHING_UP

        # PROCESS
        return PROCESS, None

    def validate_old_view_prep_prepare_rep(self, msg: OldViewPrePrepareReply):
        inst_id = getattr(msg, f.INST_ID.nm, None)

        # Check INSTANCE_ID
        if inst_id is None or inst_id != self._data.inst_id:
            return DISCARD, INCORRECT_INSTANCE

        if not self._data.is_master:
            return DISCARD, NON_MASTER

        # Check if waiting for new view
        if self._data.waiting_for_new_view:
            return STASH_VIEW_3PC, WAITING_FOR_NEW_VIEW

        # Check if catchup is in progress
        if not self._data.is_participating:
            return STASH_CATCH_UP, CATCHING_UP

        # PROCESS
        return PROCESS, None

    def _validate_3pc(self, msg) -> Tuple[int, Optional[str]]:
        pp_seq_no = getattr(msg, f.PP_SEQ_NO.nm, None)
        view_no = getattr(msg, f.VIEW_NO.nm, None)
        inst_id = getattr(msg, f.INST_ID.nm, None)

        # Check INSTANCE_ID
        if inst_id is None or inst_id != self._data.inst_id:
            return DISCARD, INCORRECT_INSTANCE

        # DISCARD CHECKS first
        # pp_seq_no cannot be 0
        if pp_seq_no == 0:
            return DISCARD, INCORRECT_PP_SEQ_NO

        # Check if below lower watermark (meaning it's already ordered)
        if pp_seq_no <= self._data.low_watermark:
            return DISCARD, ALREADY_ORDERED

        # Default checks next
        res, reason = self._validate_base(msg, view_no)
        if res != PROCESS:
            return res, reason

        # STASH CHECKS finally

        # Check if waiting for new view
        if self._data.waiting_for_new_view:
            return STASH_VIEW_3PC, WAITING_FOR_NEW_VIEW

        # Check if above high watermarks
        if pp_seq_no is not None and pp_seq_no > self._data.high_watermark:
            return STASH_WATERMARKS, OUTSIDE_WATERMARKS

        # Allow re-order already ordered if we are re-ordering in new view
        # (below prepared certificate from the previous view).
        if self.has_already_ordered(view_no, pp_seq_no):
            if pp_seq_no > self._data.prev_view_prepare_cert:
                return DISCARD, ALREADY_ORDERED

        # Stash 3PC msgs from a new view until the first batch in the view is ordered
        if pp_seq_no is not None and view_no > 0 and pp_seq_no > self._data.prev_view_prepare_cert + 1 \
                and self._data.last_ordered_3pc[1] < self._data.prev_view_prepare_cert + 1:
            return STASH_WAITING_FIRST_BATCH_IN_VIEW, WAITING_FIRST_BATCH_IN_VIEW

        # PROCESS
        return PROCESS, None

    def _validate_base(self, msg, view_no) -> Tuple[int, Optional[str]]:
        # DISCARD CHECKS

        # Check if from old view
        if view_no < self._data.view_no:
            return DISCARD, OLD_VIEW

        # STASH CHECKS

        # Check if from future view
        if view_no > self._data.view_no:
            return STASH_VIEW_3PC, FUTURE_VIEW

        # Check if catchup is in progress
        if not self._data.is_participating:
            return STASH_CATCH_UP, CATCHING_UP

        # PROCESS
        return PROCESS, None

    def has_already_ordered(self, view_no, pp_seq_no):
        return compare_3PC_keys((view_no, pp_seq_no),
                                self._data.last_ordered_3pc) >= 0
