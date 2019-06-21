from typing import List, Optional

from plenum.common.messages.node_messages import Checkpoint, PrePrepare
from plenum.server.quorums import Quorums


class ConsensusDataProvider:
    """
    This is a 3PC-state shared between Ordering, Checkpoint and ViewChange services.
    TODO: Consider depending on audit ledger
    TODO: Consider adding persistent local storage for 3PC certificates
    """
    def __init__(self, name: str, validators: List[str]):
        self._name = name
        self.set_validators(validators)
        self.view_no = 0
        self.waiting_for_new_view = False

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

    # TODO: This needs to use real primary decider implementation
    def primary_name(self, view_no: Optional[int] = None) -> str:
        if view_no is None:
            view_no = self.view_no
        return self._validators[view_no % self.quorums.n]

    def is_primary(self, view_no: Optional[int] = None) -> bool:
        return self.primary_name(view_no) == self.name

    @property
    def preprepared(self) -> List[PrePrepare]:
        """
        List of PrePrepare messages, for which quorum of Prepare messages is not reached yet
        """
        return []

    @property
    def prepared(self) -> List[PrePrepare]:
        """
        List of PrePrepare messages, for which quorum of Prepare messages is reached
        """
        return []

    @property
    def stable_checkpoint(self) -> int:
        return 0

    @property
    def checkpoints(self) -> List[Checkpoint]:
        return []
