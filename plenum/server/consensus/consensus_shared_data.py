from typing import List

from plenum.common.config_util import getConfig
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
        self.waiting_for_new_view = False
        self.primaries = []

        self.legacy_vc_in_progress = False
        self.requests = Requests()
        self.last_ordered_3pc = (0, 0)
        self.primary_name = None
        # seqNoEnd of the last stabilized checkpoint
        self.stable_checkpoint = 0
        # Checkpoint messages which the current node sent.
        self.checkpoints = SortedListWithKey(key=lambda checkpoint: checkpoint.seqNoEnd)
        # List of PrePrepare messages, for which quorum of Prepare messages is not reached yet
        self.preprepared = []  # type:  List[PrePrepare]
        # List of PrePrepare messages, for which quorum of Prepare messages is reached
        self.prepared = []  # type:  List[PrePrepare]
        self._validators = None
        self._quorums = None
        self.set_validators(validators)
        self._low_watermark = 0
        self.log_size = getConfig().LOG_SIZE
        self.high_watermark = self.low_watermark + self.log_size
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

    @property
    def is_participating(self):
        return self.node_mode == Mode.participating

    @property
    def is_synced(self):
        return Mode.is_done_syncing(self.node_mode)

    @property
    def total_nodes(self):
        return len(self.validators)

    @property
    def last_checkpoint(self) -> Checkpoint:
        if not self.checkpoints:
            return None
        else:
            return self.checkpoints[-1]

    @property
    def low_watermark(self):
        return self._low_watermark

    @low_watermark.setter
    def low_watermark(self, value: int):
        self._low_watermark = value
        self.high_watermark = value + self.log_size
        # self.logger.info('{} set watermarks as {} {}'.format(self, self.h, self.H))
        # self.stasher.unstash_watermarks()
