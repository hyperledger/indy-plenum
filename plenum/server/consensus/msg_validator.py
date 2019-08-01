from abc import ABCMeta, abstractmethod

from plenum.common.messages.node_messages import Commit, Checkpoint
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys, SortedDict
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.replica_validator_enums import DISCARD, INCORRECT_PP_SEQ_NO, ALREADY_ORDERED, STASH_VIEW, \
    FUTURE_VIEW, OLD_VIEW, GREATER_PREP_CERT, PROCESS, STASH_CATCH_UP, CATCHING_UP, STASH_WATERMARKS, \
    OUTSIDE_WATERMARKS, INCORRECT_INSTANCE, ALREADY_STABLE


class AbstractMsgValidator(metaclass=ABCMeta):
    def __init__(self, data: ConsensusSharedData):
        self._data = data

    @property
    def view_no(self):
        return self._data.view_no

    @property
    def inst_id(self):
        return self._data.inst_id

    @property
    def low_watermark(self):
        return self._data.low_watermark

    @property
    def high_watermark(self):
        return self._data.high_watermark

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
    def last_ordered_3pc(self):
        return self._data.last_ordered_3pc

    @property
    def is_participating(self):
        return self._data.is_participating

    @property
    def legacy_vc_in_progress(self):
        return self._data.legacy_vc_in_progress

    def has_already_ordered(self, view_no, pp_seq_no):
        return compare_3PC_keys((view_no, pp_seq_no),
                                self.last_ordered_3pc) >= 0

    @abstractmethod
    def validate(self, msg):
        pass


class ThreePCMsgValidator(AbstractMsgValidator):

    def validate(self, msg):
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
            if not self.legacy_vc_in_progress:
                return DISCARD, OLD_VIEW
            if self._data.legacy_last_prepared_before_view_change is None:
                return DISCARD, OLD_VIEW
            if compare_3PC_keys((view_no, pp_seq_no), self._data.legacy_last_prepared_before_view_change) < 0:
                return DISCARD, GREATER_PREP_CERT
        if view_no == self.view_no and self.legacy_vc_in_progress:
            return STASH_VIEW, FUTURE_VIEW

        # ToDo: we assume, that only is_participating needs checking orderability
        # If Catchup in View Change finished then process Commit messages
        if self._data.is_synced and self.legacy_vc_in_progress:
            return PROCESS, None

        # 5. Check if Participating
        if not self.is_participating:
            return STASH_CATCH_UP, CATCHING_UP

        # 6. Check watermarks
        if not (self.low_watermark < pp_seq_no <= self.high_watermark):
            return STASH_WATERMARKS, OUTSIDE_WATERMARKS

        return PROCESS, None


class CheckpointMsgValidator(AbstractMsgValidator):

    def validate(self, msg):
        inst_id = getattr(msg, f.INST_ID.nm, None)
        view_no = getattr(msg, f.VIEW_NO.nm, None)

        # 1. Check INSTANCE_ID
        if inst_id is None or inst_id != self.inst_id:
            return DISCARD, INCORRECT_INSTANCE

        # 2. Check if already stable
        if self._is_pp_seq_no_stable(msg):
            return DISCARD, ALREADY_STABLE

        # 3. Check if from old view
        if view_no < self.view_no:
            return DISCARD, OLD_VIEW

        # 4. Check if from future view
        if view_no > self.view_no:
            return STASH_VIEW, FUTURE_VIEW
        if view_no == self.view_no and self.legacy_vc_in_progress:
            return STASH_VIEW, FUTURE_VIEW

        # 3. Check if Participating
        if not self.is_participating:
            return STASH_CATCH_UP, CATCHING_UP

        return PROCESS, None

    def _is_pp_seq_no_stable(self, msg: Checkpoint):
        """
        :param ppSeqNo:
        :return: True if ppSeqNo is less than or equal to last stable
        checkpoint, false otherwise
        """
        return msg.seqNoEnd <= self._data.stable_checkpoint
