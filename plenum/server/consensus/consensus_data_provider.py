from typing import List

from plenum.common.messages.node_messages import PrePrepare, Checkpoint
from plenum.server.quorums import Quorums


class ConsensusDataProvider:
    """
    This is a 3PC-state shared between Ordering, Checkpoint and ViewChange services.
    TODO: Consider depending on audit ledger
    TODO: Consider adding persistent local storage for 3PC certificates
    TODO: Restore primary name from audit ledger instead of passing through constructor
    """
    def __init__(self, name: str, validators: List[str], primary_name: str):
        self._name = name
        self.set_validators(validators)
        self.view_no = 0
        self.waiting_for_new_view = False
        self.primary_name = primary_name
        self.primaries = []

        self._legacy_vc_in_progress = False
        self._is_participating = False
        self._requests = []
        self._last_ordered_3pc = (0, 0)

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
        TODO: Make it simple field?
        """
        return []

    @property
    def prepared(self) -> List[PrePrepare]:
        """
        List of PrePrepare messages, for which quorum of Prepare messages is reached
        TODO: Make it simple field?
        """
        return []

    @property
    def stable_checkpoint(self) -> int:
        return 0

    @property
    def checkpoints(self) -> List[Checkpoint]:
        return []

    """
    Needs for OrderingService
    """
    @property
    def legacy_vc_in_progress(self):
        return self._legacy_vc_in_progress

    @legacy_vc_in_progress.setter
    def legacy_vc_in_progress(self, vc_status: bool):
        self._legacy_vc_in_progress = vc_status

    @property
    def is_participating(self):
        return self._is_participating

    @is_participating.setter
    def is_participating(self, particip_status: bool):
        self._is_participating = particip_status

    @property
    def low_watermark(self):
        pass

    @property
    def high_watermark(self):
        pass

    @property
    def requests(self):
        return self._requests

    @property
    def last_ordered_3pc(self) -> tuple:
        return self._last_ordered_3pc

    @last_ordered_3pc.setter
    def last_ordered_3pc(self, key3PC):
        self._last_ordered_3pc = key3PC
        self.logger.info('{} set last ordered as {}'.format(
            self, self._last_ordered_3pc))
