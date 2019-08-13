from plenum.common.messages.node_messages import PrePrepare, Commit, Prepare, NewView
from plenum.common.stashing_router import DISCARD, PROCESS
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.replica_validator_enums import STASH_WAITING_NEW_VIEW , STASH_WATERMARKS, STASH_VIEW, STASH_CATCH_UP


class OrderingServiceMsgValidator:

    def __init__(self, data: ConsensusSharedData):
        self._data = data

    def validate_pre_prepare(self, msg: PrePrepare):
        # we discard already ordered PrePrepares
        # check for discarded first of all
        if self._has_already_ordered(msg):
            return DISCARD

        return self._validate_3pc(msg)

    def validate_prepare(self, msg: Prepare):
        # process Prepares that have been already ordered
        # to re-order batches from NEW_VIEW msg
        # prepares below low watermark have been discarded anyway
        return self._validate_3pc(msg)

    def validate_commit(self, msg: Commit):
        # process Prepares that have been already ordered
        # to re-order batches from NEW_VIEW msg
        # prepares below low watermark have been discarded anyway
        return self._validate_3pc(msg)

    def validate_new_view(self, msg: NewView):
        # View Change service has already validated NewView
        # so basic validation here is sufficient
        return self._validate_base(msg)

    def _validate_3pc(self, msg):
        pp_seq_no = getattr(msg, f.PP_SEQ_NO.nm, None)

        # DISCARD CHECKS first

        # Check if below lower watermark (meaning it's already ordered)
        if pp_seq_no <= self._data.low_watermark:
            return DISCARD

        # Default checks next
        res = self._validate_base(msg)
        if res != PROCESS:
            return res

        # STASH CHECKS finally

        # Check if waiting for new view
        if self._data.waiting_for_new_view:
            return STASH_WAITING_NEW_VIEW

        # Check if above high watermarks
        if pp_seq_no is not None and pp_seq_no > self._data.high_watermark:
            return STASH_WATERMARKS

        # PROCESS
        return PROCESS

    def _validate_base(self, msg):
        view_no = getattr(msg, f.VIEW_NO.nm, None)

        # DISCARD CHECKS

        # Check if from old view
        if view_no < self._data.view_no:
            return DISCARD

        # STASH CHECKS

        # Check if from future view
        if view_no > self._data.view_no:
            return STASH_VIEW

        # Check if catchup is in progress
        if not self._data.is_participating:
            return STASH_CATCH_UP

        # PROCESS
        return PROCESS

    def _has_already_ordered(self, msg):
        view_no = getattr(msg, f.VIEW_NO.nm, None)
        pp_seq_no = getattr(msg, f.PP_SEQ_NO.nm, None)
        return compare_3PC_keys((view_no, pp_seq_no),
                                self._data.last_ordered_3pc) >= 0
