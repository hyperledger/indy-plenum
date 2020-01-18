from typing import List, Optional, Callable

from plenum.common.config_util import getConfig
from plenum.common.messages.node_messages import PrePrepare, Checkpoint
from sortedcontainers import SortedListWithKey

from plenum.common.startable import Mode, Status
from plenum.common.util import SortedDict
from plenum.server.consensus.batch_id import BatchID
from plenum.server.consensus.view_change_storages import ViewChangeVotesForView, NewViewVotesForView
from plenum.server.models import Prepares, Commits
from plenum.server.propagator import Requests
from plenum.server.quorums import Quorums
from stp_core.common.log import getlogger

logger = getlogger()


class ConsensusSharedData:
    """
    This is a 3PC-state shared between Ordering, Checkpoint and ViewChange services.
    TODO: Consider depending on audit ledger
    TODO: Consider adding persistent local storage for 3PC certificates
    TODO: Restore primary name from audit ledger instead of passing through constructor
    """

    def __init__(self, name: str, validators: List[str], inst_id: int, is_master: bool = True):
        self._name = name
        self.inst_id = inst_id
        self.view_no = 0
        self.waiting_for_new_view = False
        self.is_master = is_master

        self.legacy_vc_in_progress = False
        self.requests = Requests()
        self.last_ordered_3pc = (0, 0)
        # Indicates name of the primary replica of this protocol instance.
        # None in case the replica does not know who the primary of the
        # instance is
        # TODO: Replace this by read-only property which uses primaries and inst_id
        self.primary_name = None
        # seqNoEnd of the last stabilized checkpoint
        self.stable_checkpoint = 0
        # Checkpoint messages which the current node sent.
        # TODO: Replace sorted list with dict
        self.checkpoints = SortedListWithKey(key=lambda checkpoint: checkpoint.seqNoEnd)
        self.checkpoints.append(self.initial_checkpoint)
        # List of BatchIDs of PrePrepare messages for which quorum of Prepare messages is not reached yet
        self.preprepared = []  # type:  List[BatchID]
        # List of BatchIDs of PrePrepare messages for which quorum of Prepare messages is reached
        self.prepared = []  # type:  List[BatchID]
        self._validators = None
        self.quorums = None
        self.new_view_votes = NewViewVotesForView(Quorums(len(validators)))
        self.view_change_votes = ViewChangeVotesForView(Quorums(len(validators)))
        # a list of validator node names ordered by rank (historical order of adding)
        self.set_validators(validators)
        self.low_watermark = 0
        self.log_size = getConfig().LOG_SIZE
        self.high_watermark = self.low_watermark + self.log_size
        self.pp_seq_no = 0
        self.node_mode = Mode.starting
        self.node_status = Status.starting
        # ToDo: it should be set in view_change_service before view_change starting
        # 3 phase key for the last prepared certificate before view change
        # started, applicable only to master instance
        self.legacy_last_prepared_before_view_change = None
        self.prev_view_prepare_cert = 0

        # Dictionary of sent PRE-PREPARE that are stored by primary replica
        # which it has broadcasted to all other non primary replicas
        # Key of dictionary is a 2 element tuple with elements viewNo,
        # pre-prepare seqNo and value is the received PRE-PREPARE
        self.sent_preprepares = SortedDict(lambda k: (k[0], k[1]))
        # type: Dict[Tuple[int, int], PrePrepare]

        # Dictionary of all Prepare requests. Key of dictionary is a 2
        # element tuple with elements viewNo, seqNo and value is a 2 element
        # tuple containing request digest and set of sender node names(sender
        # replica names in case of multiple protocol instances)
        # (viewNo, seqNo) -> ((identifier, reqId), {senders})
        self.prepares = Prepares()
        # type: Dict[Tuple[int, int], Tuple[Tuple[str, int], Set[str]]]

        self.commits = Commits()
        # type: Dict[Tuple[int, int], Tuple[Tuple[str, int], Set[str]]]

        # Tracks for which keys PRE-PREPAREs have been requested.
        # Cleared in `gc`
        self.requested_pre_prepares = {}

        # Timestamp of last ordered batch, used for freshness checks
        self.last_batch_timestamp = None

        # Flag to mark that master reordered after VC
        self.master_reordered_after_vc = True

    @property
    def name(self) -> str:
        return self._name

    def set_validators(self, validators: List[str]):
        logger.info("{} updated validators list to {}".format(self.name, validators))
        self._validators = validators
        # TODO: INDY-2263 For some reason test_send_txns_bls_consensus fails without this check
        if self.quorums is None or self.quorums.n != len(validators):
            self.quorums = Quorums(len(validators))
        self.view_change_votes.update_quorums(self.quorums)
        self.new_view_votes.update_quorums(self.quorums)

    @property
    def validators(self) -> List[str]:
        """
        List of validator nodes aliases
        """
        return self._validators

    @property
    def is_primary(self) -> Optional[bool]:
        """
        TODO: It would be much more clear and easy to use if this returned just bool.
        Returns is replica primary for this instance.
        If primary name is not defined yet, returns None
        """
        return None if self.primary_name is None else self.primary_name == self.name

    @property
    def is_participating(self):
        return self.node_mode == Mode.participating

    @property
    def is_synced(self):
        return Mode.is_done_syncing(self.node_mode)

    @property
    def is_ready(self):
        return self.node_status in Status.ready()

    @property
    def total_nodes(self):
        return len(self.validators)

    @property
    def initial_checkpoint(self):
        return Checkpoint(instId=self.inst_id, viewNo=0, seqNoStart=0, seqNoEnd=0, digest=None)

    @property
    def last_checkpoint(self) -> Checkpoint:
        return self.checkpoints[-1]

    @property
    def new_view(self):
        return self.new_view_votes.get_new_view(self.primary_name)
