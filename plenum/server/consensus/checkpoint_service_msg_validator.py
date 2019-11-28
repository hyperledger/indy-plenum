from plenum.common.messages.node_messages import Checkpoint
from plenum.common.stashing_router import PROCESS, DISCARD
from plenum.common.types import f
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.replica_validator_enums import OLD_VIEW, STASH_CATCH_UP, CATCHING_UP, \
    INCORRECT_INSTANCE, ALREADY_STABLE, WAITING_FOR_NEW_VIEW, STASH_VIEW_3PC


class CheckpointMsgValidator:

    def __init__(self, data: ConsensusSharedData):
        self._data = data

    def validate(self, msg):
        inst_id = getattr(msg, f.INST_ID.nm, None)
        view_no = getattr(msg, f.VIEW_NO.nm, None)

        # 1. Check INSTANCE_ID
        if inst_id is None or inst_id != self._data.inst_id:
            return DISCARD, INCORRECT_INSTANCE

        # 2. Check if already stable
        if self._is_pp_seq_no_stable(msg):
            return DISCARD, ALREADY_STABLE

        # 3. Check if from old view
        if view_no < self._data.view_no:
            return DISCARD, OLD_VIEW

        # 4. Check if view change is in progress
        if self._data.waiting_for_new_view:
            return STASH_VIEW_3PC, WAITING_FOR_NEW_VIEW

        # 3. Check if Participating
        if not self._data.is_participating:
            return STASH_CATCH_UP, CATCHING_UP

        return PROCESS, None

    def _is_pp_seq_no_stable(self, msg: Checkpoint):
        """
        :param ppSeqNo:
        :return: True if ppSeqNo is less than or equal to last stable
        checkpoint, false otherwise
        """
        return msg.seqNoEnd <= self._data.stable_checkpoint
