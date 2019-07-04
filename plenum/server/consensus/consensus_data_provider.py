from typing import List

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

    def __init__(self, name: str):
        self._name = name
        self.view_no = 0
        self.pp_seq_no = 0
        self.waiting_for_new_view = False
        self.primary_name = None
        self._preprepared = []
        self._prepared = []

    @property
    def name(self) -> str:
        return self._name

    def set_validators(self, validators: List[str]):
        self._validators = validators
        self._quorums = Quorums(len(validators))

    def set_primary_name(self, primary_name: str):
        self.primary_name = primary_name

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
        return 0

    @property
    def checkpoints(self) -> List[Checkpoint]:
        return []

    def preprepare_batch(self, pp: PrePrepare):
        """
        After pp had validated, it placed into _preprepared list
        """
        self._preprepared.append(pp)

    def prepare_batch(self, pp: PrePrepare):
        """
        After prepared certificate for pp had collected,
        it removed from _preprepared and placed into _prepared list
        """
        if pp is None:
            raise LogicError('pp does not exist')
        if pp not in self._preprepared:
            raise LogicError('Unprepared pp must be stored in preprepared')
        self._preprepared.remove(pp)
        self._prepared.append(pp)

    def free_batch(self, pp):
        """
        When 3pc batch had committed, rejected or catchuped till, it removed from _prepared list
        """
        if pp is None:
            raise LogicError('pp does not exist')
        if pp in self._preprepared:
            raise LogicError('Prepared pp cannot be stored in preprepared')
        if pp not in self._prepared:
            raise LogicError('Prepared pp must be stored in prepared')
        self._prepared.remove(pp)

    def free_all(self):
        """
        When catchup finished, backups clear up their pre-prepares
        """
        self._prepared.clear()
        self._preprepared.clear()

    def get_3pc(self):
        return self.view_no, self.pp_seq_no

    def set_3pc(self, key):
        if len(key) != 2:
            raise LogicError('3pc key must be a pair')
        self.view_no = key[0]
        self.pp_seq_no = key[1]
