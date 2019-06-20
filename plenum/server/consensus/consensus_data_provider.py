from typing import List

from plenum.common.messages.node_messages import Checkpoint, PrePrepare


class ConsensusDataProvider:
    """
    This is a 3PC-state shared between Ordering, Checkpoint and ViewChange services.
    TODO: Consider depending on audit ledger
    TODO: Consider adding persistent local storage for 3PC certificates
    """
    def __init__(self, name):
        self._name = name
        self.view_no = 0
        self.waiting_for_new_view = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def primary_name(self) -> str:
        return 'Alpha'  # TODO

    @property
    def is_primary(self) -> bool:
        return self.primary_name == self.name

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
