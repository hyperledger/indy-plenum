from typing import List

from sortedcontainers import SortedListWithKey

from plenum.common.util import SortedDict

from common.exceptions import LogicError

from plenum.common.messages.node_messages import Checkpoint, PrePrepare
from plenum.server.quorums import Quorums


class ConsensusDataProvider:
    """
    This is a 3PC-state shared between Ordering, Checkpoint and ViewChange services.
    TODO: Consider depending on audit ledger
    TODO: Consider adding persistent local storage for 3PC certificates
    TODO: Restore primary name from audit ledger instead of passing through constructor
    """

    def __init__(self, name: str, validators: List[str]):
        self._name = name
        self.view_no = 0
        self.pp_seq_no = 0
        self.waiting_for_new_view = False
        self.primary_name = None
        self.set_validators(validators)
        self._stable_checkpoint = None
        self._checkpoints = SortedListWithKey(lambda checkpoint: checkpoint.seqNoEnd)
        self._preprepared = []
        self._prepared = []

    @property
    def name(self) -> str:
        return self._name

    def set_validators(self, validators: List[str]):
        self._validators = validators
        self._quorums = Quorums(len(validators))

    @property
    def validators(self) -> List[str]:
        """
        List of validator nodes aliases
        """
        return self._validators

    @property
    def quorums(self) -> Quorums:
        """
        List of quorums
        """
        return self._quorums

    @property
    def is_primary(self) -> bool:
        return self.primary_name == self.name

    @property
    def preprepared(self) -> List[PrePrepare]:
        """
        List of PrePrepare messages, for which quorum of Prepare messages is not reached yet
        """
        return self._preprepared

    @property
    def prepared(self) -> List[PrePrepare]:
        """
        List of PrePrepare messages, for which quorum of Prepare messages is reached
        """
        return self._prepared

    @property
    def stable_checkpoint(self) -> int:
        return self._stable_checkpoint

    @property
    def checkpoints(self) -> SortedListWithKey:
        """
        List of Checkpoints, which has been sent, but not stabilized yet
        """
        return self._checkpoints

    def preprepare_batch(self, pp: PrePrepare):
        """
        After pp had validated, it placed into _preprepared list
        """
        # TODO: move logic checks upper the functions stack
        if pp in self._preprepared:
            raise LogicError('New pp cannot be stored in preprepared')
        if pp in self._prepared:
            raise LogicError('New pp cannot be stored in prepared')
        if pp.ppSeqNo < self._checkpoints[-1].seqNoEnd:
            raise LogicError('ppSeqNo cannot be lower than last checkpoint')

        self._preprepared.append(pp)

    def prepare_batch(self, pp: PrePrepare):
        """
        After prepared certificate for pp had collected,
        it removed from _preprepared and placed into _prepared list
        """
        # TODO: move logic checks upper the functions stack
        if pp not in self._preprepared:
            raise LogicError('Unprepared pp must be stored in preprepared')
        self._preprepared.remove(pp)
        self._prepared.append(pp)

    def clear_batch(self, pp: PrePrepare):
        """
        When 3pc batch processed, it removed from _prepared list
        """
        # TODO: move logic checks upper the functions stack
        if pp in self._preprepared == pp not in self._prepared:
            raise LogicError('Batch can be only in one condition while clearing ')
        self._prepared.remove(pp)

    def clear_all_batches(self):
        """
        Clear up all preprepared and prepared
        """
        self._prepared.clear()
        self._preprepared.clear()

    def append_checkpoint(self, checkpoint: Checkpoint):
        self._checkpoints.add(checkpoint)

    def remove_checkpoint(self, end_seq_no):
        new_checkpoints = [c for c in self._checkpoints if c.seqNoEnd != end_seq_no]
        if len(new_checkpoints) != len(self._checkpoints) - 1:
            raise LogicError('One checkpoint needed to be removed')
        self._checkpoints = new_checkpoints

    def remove_all_checkpoints(self):
        # That function most probably redundant in PBFT approach,
        # because according to paper, checkpoints cleared only when next stabilized.
        # Avoid using it while implement other services.
        self._checkpoints.clear()

    def set_stable_checkpoint(self, end_seq_no):
        for c in self._checkpoints:
            if c.seqNoEnd == end_seq_no:
                self._stable_checkpoint = end_seq_no
                break
        else:
            raise LogicError('Stable checkpoint must be in checkpoints')
