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
        self._view_no = 0
        self._waiting_for_new_view = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def primary_name(self) -> str:
        return 'some_primary'  # TODO

    @property
    def view_no(self) -> int:
        return self._view_no

    @property
    def waiting_for_new_view(self) -> bool:
        return self._waiting_for_new_view

    @property
    def preprepared(self) -> List[PrePrepare]:
        return []

    @property
    def prepared(self) -> List[PrePrepare]:
        return []

    @property
    def stable_checkpoint(self) -> int:
        return 0

    @property
    def checkpoints(self) -> List[Checkpoint]:
        return []

    def enter_next_view(self):
        self._view_no += 1
        self._waiting_for_new_view = True
