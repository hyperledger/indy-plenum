from collections import defaultdict

import sys
from typing import Tuple, NamedTuple, List, Set

from common.exceptions import LogicError
from plenum.common.config_util import getConfig
from plenum.common.constants import AUDIT_LEDGER_ID, AUDIT_TXN_VIEW_NO, AUDIT_TXN_PP_SEQ_NO
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.ledger import Ledger
from plenum.common.messages.internal_messages import NeedMasterCatchup, NeedBackupCatchup, CheckpointStabilized, \
    BackupSetupLastOrdered, NewViewAccepted, NewViewCheckpointsApplied, CatchupFinished, CatchupCheckpointsApplied
from plenum.common.messages.node_messages import Checkpoint, Ordered
from plenum.common.metrics_collector import MetricsName, MetricsCollector, NullMetricsCollector
from plenum.common.router import Subscription
from plenum.common.stashing_router import StashingRouter, PROCESS
from plenum.common.txn_util import get_payload_data
from plenum.common.util import compare_3PC_keys
from plenum.server.consensus.checkpoint_service_msg_validator import CheckpointMsgValidator
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.metrics_decorator import measure_consensus_time
from plenum.server.database_manager import DatabaseManager
from plenum.server.replica_validator_enums import STASH_WATERMARKS
from stp_core.common.log import getlogger

logger = getlogger()


class CheckpointService:
    STASHED_CHECKPOINTS_BEFORE_CATCHUP = 1

    # TODO: Remove view_no from key after implementing INDY-1336
    CheckpointKey = NamedTuple('CheckpointKey',
                               [('view_no', int), ('pp_seq_no', int), ('digest', str)])

    def __init__(self, data: ConsensusSharedData, bus: InternalBus, network: ExternalBus,
                 stasher: StashingRouter, db_manager: DatabaseManager,
                 metrics: MetricsCollector = NullMetricsCollector(), ):
        self._data = data
        self._bus = bus
        self._network = network
        self._stasher = stasher
        self._subscription = Subscription()
        self._validator = CheckpointMsgValidator(self._data)
        self._db_manager = db_manager
        self.metrics = metrics

        # Received checkpoints, mapping CheckpointKey -> List(node_alias)
        self._received_checkpoints = defaultdict(set)  # type: Dict[CheckpointService.CheckpointKey, Set[str]]

        self._config = getConfig()

        self._subscription.subscribe(stasher, Checkpoint, self.process_checkpoint)

        self._subscription.subscribe(bus, Ordered, self.process_ordered)
        self._subscription.subscribe(bus, BackupSetupLastOrdered, self.process_backup_setup_last_ordered)
        self._subscription.subscribe(bus, NewViewAccepted, self.process_new_view_accepted)
        self._subscription.subscribe(bus, CatchupFinished, self.process_catchup_finished)

    def cleanup(self):
        self._subscription.unsubscribe_all()

    @property
    def view_no(self):
        return self._data.view_no

    @property
    def is_master(self):
        return self._data.is_master

    @property
    def last_ordered_3pc(self):
        return self._data.last_ordered_3pc

    @measure_consensus_time(MetricsName.PROCESS_CHECKPOINT_TIME,
                            MetricsName.BACKUP_PROCESS_CHECKPOINT_TIME)
    def process_checkpoint(self, msg: Checkpoint, sender: str) -> (bool, str):
        """
        Process checkpoint messages
        :return: whether processed (True) or stashed (False)
        """
        logger.info('{} processing checkpoint {} from {}'.format(self, msg, sender))
        result, reason = self._validator.validate(msg)
        if result != PROCESS:
            return result, reason

        key = self._checkpoint_key(msg)
        self._received_checkpoints[key].add(sender)
        self._try_to_stabilize_checkpoint(key)
        self._start_catchup_if_needed(key)

    def process_backup_setup_last_ordered(self, msg: BackupSetupLastOrdered):
        if msg.inst_id != self._data.inst_id:
            return
        self.update_watermark_from_3pc()

    def process_ordered(self, ordered: Ordered):
        for batch_id in reversed(self._data.preprepared):
            if batch_id.pp_seq_no == ordered.ppSeqNo:
                self._add_to_checkpoint(batch_id.pp_seq_no,
                                        batch_id.view_no,
                                        ordered.auditTxnRootHash)
                return
        raise LogicError("CheckpointService | Can't process Ordered msg because "
                         "ppSeqNo {} not in preprepared".format(ordered.ppSeqNo))

    def _start_catchup_if_needed(self, key: CheckpointKey):
        if self._have_own_checkpoint(key):
            return

        unknown_stabilized = self._unknown_stabilized_checkpoints()
        lag_in_checkpoints = len(unknown_stabilized)
        if lag_in_checkpoints <= self.STASHED_CHECKPOINTS_BEFORE_CATCHUP:
            return

        last_key = sorted(unknown_stabilized, key=lambda v: (v.view_no, v.pp_seq_no))[-1]

        if self.is_master:
            if not self._data.is_primary:
                logger.display('{} has lagged for {} checkpoints so the catchup procedure starts'.
                               format(self, lag_in_checkpoints))
                self._bus.send(NeedMasterCatchup())
        else:
            logger.info('{} has lagged for {} checkpoints so adjust last_ordered_3pc to {}, '
                        'shift watermarks and clean collections'.
                        format(self, lag_in_checkpoints, last_key.pp_seq_no))
            # Adjust last_ordered_3pc, shift watermarks, clean operational
            # collections and process stashed messages which now fit between
            # watermarks
            # TODO: Actually we might need to process view_no from last_key as well, however
            #  it wasn't processed before, and it will go away when INDY-1336 gets implemented
            key_3pc = (self.view_no, last_key.pp_seq_no)
            self._bus.send(NeedBackupCatchup(inst_id=self._data.inst_id,
                                             caught_up_till_3pc=key_3pc))
            self.caught_up_till_3pc(key_3pc)

    def caught_up_till_3pc(self, caught_up_till_3pc):
        cp_seq_no = caught_up_till_3pc[1] // self._config.CHK_FREQ * self._config.CHK_FREQ
        self._mark_checkpoint_stable(cp_seq_no)

    def catchup_clear_for_backup(self):
        self._reset_checkpoints()
        self._remove_received_checkpoints()
        self.set_watermarks(low_watermark=0,
                            high_watermark=sys.maxsize)

    def _add_to_checkpoint(self, ppSeqNo, view_no, audit_txn_root_hash):
        if ppSeqNo % self._config.CHK_FREQ != 0:
            return

        key = self.CheckpointKey(view_no=view_no,
                                 pp_seq_no=ppSeqNo,
                                 digest=audit_txn_root_hash)

        self._do_checkpoint(ppSeqNo, view_no, audit_txn_root_hash)
        self._try_to_stabilize_checkpoint(key)

    @measure_consensus_time(MetricsName.SEND_CHECKPOINT_TIME,
                            MetricsName.BACKUP_SEND_CHECKPOINT_TIME)
    def _do_checkpoint(self, pp_seq_no, view_no, audit_txn_root_hash):
        logger.info("{} sending Checkpoint {} view {} audit txn root hash {}".
                    format(self, pp_seq_no, view_no, audit_txn_root_hash))

        checkpoint = Checkpoint(self._data.inst_id, view_no, 0, pp_seq_no, audit_txn_root_hash)
        self._network.send(checkpoint)
        self._data.checkpoints.add(checkpoint)

    def _try_to_stabilize_checkpoint(self, key: CheckpointKey):
        if not self._have_quorum_on_received_checkpoint(key):
            return

        if not self._have_own_checkpoint(key):
            return

        self._mark_checkpoint_stable(key.pp_seq_no)

    def _mark_checkpoint_stable(self, pp_seq_no):
        self._data.stable_checkpoint = pp_seq_no

        stable_checkpoints = self._data.checkpoints.irange_key(min_key=pp_seq_no, max_key=pp_seq_no)
        if len(list(stable_checkpoints)) == 0:
            checkpoint = self._create_checkpoint_from_audit_ledger(pp_seq_no)
            logger.info("{} adding a stable checkpoint {}".format(self, checkpoint))
            self._data.checkpoints.add(checkpoint)

        for cp in self._data.checkpoints.copy():
            if cp.seqNoEnd < pp_seq_no:
                logger.trace("{} removing previous checkpoint {}".format(self, cp))
                self._data.checkpoints.remove(cp)

        self.set_watermarks(low_watermark=pp_seq_no)

        self._remove_received_checkpoints(till_3pc_key=(self.view_no, pp_seq_no))
        self._bus.send(CheckpointStabilized((self.view_no, pp_seq_no)))  # call OrderingService.gc()
        logger.info("{} marked stable checkpoint {}".format(self, pp_seq_no))

    def _create_checkpoint_from_audit_ledger(self, pp_seq_no):
        audit_ledger = self._db_manager.get_ledger(AUDIT_LEDGER_ID)
        audit_txn, audit_txn_seq_no = self._audit_txn_by_pp_seq_no(audit_ledger, pp_seq_no)
        # TODO: What should we do if txn not found or audit ledger is empty?
        view_no = self._get_view_no_from_audit(audit_txn)
        digest = self._get_digest_from_audit(audit_ledger, audit_txn_seq_no)
        return Checkpoint(instId=self._data.inst_id,
                          viewNo=view_no,
                          seqNoStart=0,
                          seqNoEnd=pp_seq_no,
                          digest=digest)

    def _get_view_no_from_audit(self, audit_txn):
        return self.view_no if audit_txn is None else get_payload_data(audit_txn)[AUDIT_TXN_VIEW_NO]

    def _get_digest_from_audit(self, audit_ledger, audit_txn_seq_no):
        return None if not audit_txn_seq_no else audit_ledger.hashToStr(
            audit_ledger.tree.merkle_tree_hash(0, audit_txn_seq_no))

    def set_watermarks(self, low_watermark: int, high_watermark: int = None):
        self._data.low_watermark = low_watermark
        self._data.high_watermark = self._data.low_watermark + self._config.LOG_SIZE \
            if high_watermark is None else \
            high_watermark

        logger.info('{} set watermarks as {} {}'.format(self,
                                                        self._data.low_watermark,
                                                        self._data.high_watermark))
        self._stasher.process_all_stashed(STASH_WATERMARKS)

    def update_watermark_from_3pc(self):
        last_ordered_3pc = self.last_ordered_3pc
        if (last_ordered_3pc is not None) and (last_ordered_3pc[0] == self.view_no):
            logger.info("update_watermark_from_3pc to {}".format(last_ordered_3pc))
            self.set_watermarks(last_ordered_3pc[1])
        else:
            logger.info("try to update_watermark_from_3pc but last_ordered_3pc is None")

    def _remove_received_checkpoints(self, till_3pc_key=None):
        """
        Remove received checkpoints up to `till_3pc_key` if provided,
        otherwise remove all received checkpoints
        """
        if till_3pc_key is None:
            self._received_checkpoints.clear()
            logger.info('{} removing all received checkpoints'.format(self))
            return

        for cp in list(self._received_checkpoints.keys()):
            if self._is_below_3pc_key(cp, till_3pc_key):
                logger.info('{} removing received checkpoints: {}'.format(self, cp))
                del self._received_checkpoints[cp]

    def _reset_checkpoints(self):
        # That function most probably redundant in PBFT approach,
        # because according to paper, checkpoints cleared only when next stabilized.
        # Avoid using it while implement other services.
        self._data.checkpoints.clear()
        self._data.checkpoints.add(self._data.initial_checkpoint)

    def __str__(self) -> str:
        return "{} - checkpoint_service".format(self._data.name)

    def discard(self, msg, reason, sender):
        logger.trace("{} discard message {} from {} "
                     "with the reason: {}".format(self, msg, sender, reason))

    def _have_own_checkpoint(self, key: CheckpointKey) -> bool:
        own_checkpoints = self._data.checkpoints.irange_key(min_key=key.pp_seq_no, max_key=key.pp_seq_no)
        return any(cp.viewNo == key.view_no and cp.digest == key.digest for cp in own_checkpoints)

    def _have_quorum_on_received_checkpoint(self, key: CheckpointKey) -> bool:
        votes = self._received_checkpoints[key]
        return self._data.quorums.checkpoint.is_reached(len(votes))

    def _unknown_stabilized_checkpoints(self) -> List[CheckpointKey]:
        return [key for key in self._received_checkpoints
                if self._have_quorum_on_received_checkpoint(key) and
                not self._have_own_checkpoint(key) and
                not self._is_below_3pc_key(key, self.last_ordered_3pc)]

    @staticmethod
    def _is_below_3pc_key(cp: CheckpointKey, key: Tuple[int, int]) -> bool:
        return compare_3PC_keys((cp.view_no, cp.pp_seq_no), key) >= 0

    @staticmethod
    def _checkpoint_key(checkpoint: Checkpoint) -> CheckpointKey:
        return CheckpointService.CheckpointKey(
            view_no=checkpoint.viewNo,
            pp_seq_no=checkpoint.seqNoEnd,
            digest=checkpoint.digest
        )

    @staticmethod
    def _audit_txn_by_pp_seq_no(audit_ledger: Ledger, pp_seq_no: int) -> (dict, int):
        # TODO: Should we put it into some common code?
        seq_no = audit_ledger.size
        txn = None
        while seq_no > 0:
            txn = audit_ledger.getBySeqNo(seq_no)
            txn_data = get_payload_data(txn)
            audit_pp_seq_no = txn_data[AUDIT_TXN_PP_SEQ_NO]
            if audit_pp_seq_no == pp_seq_no:
                break
            seq_no -= 1
        return txn, seq_no

    def process_new_view_accepted(self, msg: NewViewAccepted):
        if self.is_master:
            cp = msg.checkpoint
            # do not update stable checkpoints if the node doesn't have this checkpoint
            # the node is lagging behind in this case and will start catchup after receiving
            # one more quorum of checkpoints from other nodes
            if cp not in self._data.checkpoints:
                return
            self._mark_checkpoint_stable(cp.seqNoEnd)
            self.set_watermarks(low_watermark=cp.seqNoEnd)
        else:
            # TODO: This is kind of hackery, but proper way would require introducing more
            #  messages specifically for backup replicas. Hope we can live with it for now.
            self._data.waiting_for_new_view = False
            self._data.pp_seq_no = 0
            self.set_watermarks(low_watermark=0)
            self._reset_checkpoints()
            self._data.stable_checkpoint = 0
            self._remove_received_checkpoints()

        self._bus.send(NewViewCheckpointsApplied(view_no=msg.view_no,
                                                 view_changes=msg.view_changes,
                                                 checkpoint=msg.checkpoint,
                                                 batches=msg.batches))

    def process_catchup_finished(self, msg: CatchupFinished):
        if compare_3PC_keys(msg.master_last_ordered,
                            msg.last_caught_up_3PC) > 0:
            if self._data.is_master:
                self.caught_up_till_3pc(msg.last_caught_up_3PC)
            else:
                if not self._data.is_primary:
                    self.catchup_clear_for_backup()

        self._bus.send(CatchupCheckpointsApplied(last_caught_up_3PC=msg.last_caught_up_3PC,
                                                 master_last_ordered=msg.master_last_ordered))
