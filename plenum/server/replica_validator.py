from enum import unique, IntEnum

from plenum.common.types import f
from plenum.common.util import compare_3PC_keys


@unique
class ReplicaValidationResult(IntEnum):
    PROCESS = 0
    DISCARD = 1
    STASH = 2


class ReplicaValidator:

    def __init__(self, replica) -> None:
        self.replica = replica

    @property
    def view_no(self):
        return self.replica.viewNo

    @property
    def inst_id(self):
        return self.replica.instId

    def validate(self, msg):
        node = self.replica.node
        inst_id = getattr(msg, f.INST_ID.nm, None)
        view_no = getattr(msg, f.VIEW_NO.nm, None)
        pp_seq_no = getattr(msg, f.PP_SEQ_NO.nm, None)

        # 1. Check INSTANCE_ID
        if inst_id is None or inst_id != self.replica.instId:
            return ReplicaValidationResult.DISCARD

        # 2. Check watermarks
        if pp_seq_no == 0:
            # should start with 1
            return ReplicaValidationResult.DISCARD
        if not (self.replica.h < pp_seq_no <= self.replica.H):
            return ReplicaValidationResult.STASH

        # 3. Check if Participating
        if not node.isParticipating:
            return ReplicaValidationResult.STASH

        # 4. Check viewNo
        if view_no > self.replica.viewNo:
            return ReplicaValidationResult.STASH
        if view_no < self.replica.viewNo - 1:
            return ReplicaValidationResult.DISCARD
        if view_no == self.replica.viewNo - 1:
            if not node.view_change_in_progress:
                return ReplicaValidationResult.DISCARD
            if self.replica.last_prepared_before_view_change is None:
                return ReplicaValidationResult.DISCARD
            if compare_3PC_keys((view_no, pp_seq_no), self.replica.last_prepared_before_view_change) < 0:
                return ReplicaValidationResult.DISCARD
        if view_no == self.replica.viewNo and node.view_change_in_progress:
            return ReplicaValidationResult.STASH

        # 5. Check already ordered
        if self.replica.has_already_ordered(view_no, pp_seq_no):
            return ReplicaValidationResult.DISCARD

        return ReplicaValidationResult.PROCESS
