from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.replica_validator_enums import DISCARD, INCORRECT_INSTANCE, PROCESS, ALREADY_ORDERED, FUTURE_VIEW, \
    GREATER_PREP_CERT, OLD_VIEW, CATCHING_UP, OUTSIDE_WATERMARKS, INCORRECT_PP_SEQ_NO, ALREADY_STABLE, STASH_WATERMARKS, \
    STASH_CATCH_UP, STASH_VIEW


class ReplicaValidator:

    def __init__(self, replica) -> None:
        self.replica = replica

    @property
    def view_no(self):
        return self.replica.viewNo

    @property
    def inst_id(self):
        return self.replica.instId

    def validate_3pc_msg(self, msg):
        node = self.replica.node
        inst_id = getattr(msg, f.INST_ID.nm, None)
        view_no = getattr(msg, f.VIEW_NO.nm, None)
        pp_seq_no = getattr(msg, f.PP_SEQ_NO.nm, None)

        # 1. Check INSTANCE_ID
        if inst_id is None or inst_id != self.replica.instId:
            return DISCARD, INCORRECT_INSTANCE

        # 2. Check pp_seq_no
        if pp_seq_no == 0:
            # should start with 1
            return DISCARD, INCORRECT_PP_SEQ_NO

        # 3. Check already ordered
        if self.replica.has_already_ordered(view_no, pp_seq_no):
            return DISCARD, ALREADY_ORDERED

        # 4. Check viewNo
        if view_no > self.replica.viewNo:
            return STASH_VIEW, FUTURE_VIEW
        if view_no < self.replica.viewNo - 1:
            return DISCARD, OLD_VIEW
        if view_no == self.replica.viewNo - 1:
            if not node.view_change_in_progress:
                return DISCARD, OLD_VIEW
            if self.replica.last_prepared_before_view_change is None:
                return DISCARD, OLD_VIEW
            if compare_3PC_keys((view_no, pp_seq_no), self.replica.last_prepared_before_view_change) < 0:
                return DISCARD, GREATER_PREP_CERT
        if view_no == self.replica.viewNo and node.view_change_in_progress:
            return STASH_VIEW, FUTURE_VIEW

        # If Catchup in View Change finished then process a message
        if node.is_synced and node.view_change_in_progress:
            return PROCESS, None

        # 5. Check if Participating
        if not node.isParticipating:
            return STASH_CATCH_UP, CATCHING_UP

        # 6. Check watermarks
        if not (self.replica.h < pp_seq_no <= self.replica.H):
            return STASH_WATERMARKS, OUTSIDE_WATERMARKS

        return PROCESS, None

    def validate_checkpoint_msg(self, msg):
        node = self.replica.node
        inst_id = getattr(msg, f.INST_ID.nm, None)
        view_no = getattr(msg, f.VIEW_NO.nm, None)

        # 1. Check INSTANCE_ID
        if inst_id is None or inst_id != self.replica.instId:
            return DISCARD, INCORRECT_INSTANCE

        # 2. Check if already stable
        if self.replica.is_pp_seq_no_stable(msg):
            return DISCARD, ALREADY_STABLE

        # 3. Check if from old view
        if view_no < self.replica.viewNo:
            return DISCARD, OLD_VIEW

        # 4. Check if from future view
        if view_no > self.replica.viewNo:
            return STASH_VIEW, FUTURE_VIEW
        if view_no == self.replica.viewNo and self.replica.node.view_change_in_progress:
            return STASH_VIEW, FUTURE_VIEW

        # 3. Check if Participating
        if not node.isParticipating:
            return STASH_CATCH_UP, CATCHING_UP

        return PROCESS, None
