from typing import List

from plenum.common.messages.node_messages import PrePrepare, Checkpoint
from sortedcontainers import SortedListWithKey

from plenum.common.startable import Mode
from plenum.server.propagator import Requests
from plenum.server.quorums import Quorums


class ConsensusSharedData:
    """
    This is a 3PC-state shared between Ordering, Checkpoint and ViewChange services.
    TODO: Consider depending on audit ledger
    TODO: Consider adding persistent local storage for 3PC certificates
    TODO: Restore primary name from audit ledger instead of passing through constructor
    """

    def __init__(self, name: str, validators: List[str], inst_id: int):
        self._name = name
        self.inst_id = inst_id
        self.view_no = 0
        self.last_ordered_3pc = (0, 0)
        self.waiting_for_new_view = False
        self.primaries = []

        self._legacy_vc_in_progress = False
        self._is_participating = False
        self._requests = Requests()
        self._last_ordered_3pc = (0, 0)
        self.primary_name = None
        self.stable_checkpoint = 0
        self.checkpoints = SortedListWithKey(key=lambda checkpoint: checkpoint.seqNoEnd)
        self.preprepared = []  # type:  List[PrePrepare]
        self.prepared = []  # type:  List[PrePrepare]
        self._validators = None
        self._quorums = None
        self.set_validators(validators)
        self.low_watermark = 0
        self.log_size = 300  # TODO: use config value
        self.high_watermark = self.low_watermark + self.log_size
        self._total_nodes = len(self.validators)
        self.pp_seq_no = 0
        self.node_mode = Mode.starting
        # ToDo: it should be set in view_change_service before view_change starting
        self.legacy_last_prepared_before_view_change = None

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
    def is_synced(self):
        return Mode.is_done_syncing(self.node_mode)

    @property
    def requests(self):
        return self._requests

    @property
    def last_ordered_3pc(self) -> tuple:
        return self._last_ordered_3pc

    @last_ordered_3pc.setter
    def last_ordered_3pc(self, key3PC):
        self._last_ordered_3pc = key3PC
        # ToDo: does not need on first stage
        # self.logger.info('{} set last ordered as {}'.format(
        #     self, self._last_ordered_3pc))

    @property
    def total_nodes(self):
        return self._total_nodes

    @total_nodes.setter
    def total_nodes(self, tn):
        self._total_nodes = tn

    @property
    def last_checkpoint(self) -> Checkpoint:
        return self.checkpoints[-1]
