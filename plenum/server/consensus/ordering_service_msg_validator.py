from plenum.common.stashing_router import DISCARD


class OrderingServiceMsgValidator:

    def __init__(self, data: ConsensusSharedData):
        self._data = data

    def validate_pre_prepare(self, msg: PrePrepare):
        res, reason = self._validate_3pc(msg)
        if res != PROCESS:
            return res, reason

        # we discard already ordered PrePrepares
        if self._has_already_ordered(msg):
            return DISCARD, ALREADY_ORDERED

        return PROCESS, None

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
        res, reason = self._validate_base(msg)
        if res != PROCESS:
            return res, reason

        pp_seq_no = getattr(msg, f.PP_SEQ_NO.nm, None)

        ### DISCARD CHECKS

        # Check if below lower watermark (meaning it's already ordered)
        if pp_seq_no <= self._data.low_watermark:
            return DISCARD, ALREADY_ORDERED

        ### STASH CHECKS

        # Check if waiting for new view
        if self._data.waiting_for_new_view:
            return STASH_WIATING_NEW_VIEW, WAITING_FOR_NEW_VIEW

        # Check if above high watermarks
        if pp_seq_no is not None and pp_seq_no > self._data.high_watermark:
            return STASH_WATERMARKS, OUTSIDE_WATERMARKS

        ### PROCESS
        return PROCESS, None

    def _validate_base(self, msg):
        view_no = getattr(msg, f.VIEW_NO.nm, None)

        ### DISCARD CHECKS

        # Check if from old view
        if view_no < self._data.view_no:
            return DISCARD, OLD_VIEW

        ### STASH CHECKS

        # Check if from future view
        if view_no > self._data.view_no:
            return STASH_VIEW, FUTURE_VIEW

        # Check if catchup is in progress
        if not self._data.is_participating:
            return STASH_CATCH_UP, CATCHING_UP

        ### PROCESS
        return PROCESS, None

    def _has_already_ordered(self, msg):
        view_no = getattr(msg, f.VIEW_NO.nm, None)
        pp_seq_no = getattr(msg, f.PP_SEQ_NO.nm, None)
        return compare_3PC_keys((view_no, pp_seq_no),
                                self._data.last_ordered_3pc) >= 0
