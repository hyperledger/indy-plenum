import math
import sys
from _sha256 import sha256
from typing import Tuple

from sortedcontainers import SortedListWithKey

from common.exceptions import LogicError
from common.serializers.serialization import serialize_msg_for_signing
from plenum.common.config_util import getConfig
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.internal_messages import StartMasterCatchup, StartBackupCatchup, Cleanup
from plenum.common.messages.node_messages import Checkpoint, Ordered, CheckpointState
from plenum.common.metrics_collector import MetricsName
from plenum.common.stashing_router import DISCARD, PROCESS, StashingRouter
from plenum.common.util import updateNamedTuple, SortedDict, firstKey
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.msg_validator import CheckpointMsgValidator
from plenum.server.database_manager import DatabaseManager
from plenum.server.replica_stasher import ReplicaStasher
from stp_core.common.log import getlogger


class CheckpointService:
    STASHED_CHECKPOINTS_BEFORE_CATCHUP = 1

    def __init__(self, data: ConsensusSharedData, bus: InternalBus, network: ExternalBus,
                 stasher: StashingRouter, db_manager: DatabaseManager, old_stasher: ReplicaStasher,
                 is_master=True):
        self._data = data
        self._bus = bus
        self._network = network
        self._checkpoint_state = SortedDict(lambda k: k[1])
        self._stasher = stasher
        self._is_master = is_master
        self._validator = CheckpointMsgValidator(self._data)
        self._db_manager = db_manager

        # Stashed checkpoints for each view. The key of the outermost
        # dictionary is the view_no, value being a dictionary with key as the
        # range of the checkpoint and its value again being a mapping between
        # senders and their sent checkpoint
        # Dict[view_no, Dict[(seqNoStart, seqNoEnd),  Dict[sender, Checkpoint]]]
        self._stashed_recvd_checkpoints = {}

        self._config = getConfig()
        self._logger = getlogger()

        self._old_stasher = old_stasher

        # self._stasher.subscribe(Checkpoint, self.process_checkpoint)
        # self._stasher.subscribe_to(network)
        #
        # self._bus.subscribe(Ordered, self.process_ordered)
        # self._bus.subscribe(StartBackupCatchup, self.caught_up_till_3pc)

    @property
    def view_no(self):
        return self._data.view_no

    @property
    def last_ordered_3pc(self):
        return self._data.last_ordered_3pc

    # @measure_replica_time(MetricsName.PROCESS_CHECKPOINT_TIME,
    #                       MetricsName.BACKUP_PROCESS_CHECKPOINT_TIME)
    def process_checkpoint(self, msg: Checkpoint, sender: str) -> bool:
        """
        Process checkpoint messages

        :return: whether processed (True) or stashed (False)
        """
        seqNoEnd = msg.seqNoEnd
        seqNoStart = msg.seqNoStart
        key = (seqNoStart, seqNoEnd)

        if key not in self._checkpoint_state or not self._checkpoint_state[key].digest:
            self._stash_checkpoint(msg, sender)
            self._remove_stashed_checkpoints(self.last_ordered_3pc)
            self._start_catchup_if_needed()
            return False

        checkpoint_state = self._checkpoint_state[key]
        # Raise the error only if master since only master's last
        # ordered 3PC is communicated during view change
        if self._is_master and checkpoint_state.digest != msg.digest:
            self._logger.warning("{} received an incorrect digest {} for "
                                 "checkpoint {} from {}".format(self, msg.digest, key, sender))
            return True

        checkpoint_state.receivedDigests[sender] = msg.digest
        self._check_if_checkpoint_stable(key)
        return True

    def process_ordered(self, ordered: Ordered):
        for pp in reversed(self._data.preprepared):
            if pp.ppSeqNo == ordered.ppSeqNo:
                self._add_to_checkpoint(pp.ppSeqNo, pp.digest, pp.ledgerId, pp.viewNo)
                return
        raise LogicError("CheckpointService | Can't process Ordered msg because "
                         "ppSeqNo {} not in preprepared".format(ordered.ppSeqNo))

    def _start_catchup_if_needed(self):
        stashed_checkpoint_ends = self._stashed_checkpoints_with_quorum()
        lag_in_checkpoints = len(stashed_checkpoint_ends)
        if self._checkpoint_state:
            (s, e) = firstKey(self._checkpoint_state)
            # If the first stored own checkpoint has a not aligned lower bound
            # (this means that it was started after a catch-up), is complete
            # and there is a quorumed stashed checkpoint from other replicas
            # with the same end then don't include this stashed checkpoint
            # into the lag
            if s % self._config.CHK_FREQ != 0 \
                    and self._checkpoint_state[(s, e)].seqNo == e \
                    and e in stashed_checkpoint_ends:
                lag_in_checkpoints -= 1
        is_stashed_enough = \
            lag_in_checkpoints > self.STASHED_CHECKPOINTS_BEFORE_CATCHUP
        if not is_stashed_enough:
            return

        if self._is_master:
            self._logger.display(
                '{} has lagged for {} checkpoints so updating watermarks to {}'.format(
                    self, lag_in_checkpoints, stashed_checkpoint_ends[-1]))
            self.set_watermarks(low_watermark=stashed_checkpoint_ends[-1])
            if not self._data.is_primary:
                self._logger.display(
                    '{} has lagged for {} checkpoints so the catchup procedure starts'.format(
                        self, lag_in_checkpoints))
                self._bus.send(StartMasterCatchup())
        else:
            self._logger.info(
                '{} has lagged for {} checkpoints so adjust last_ordered_3pc to {}, '
                'shift watermarks and clean collections'.format(
                    self, lag_in_checkpoints, stashed_checkpoint_ends[-1]))
            # Adjust last_ordered_3pc, shift watermarks, clean operational
            # collections and process stashed messages which now fit between
            # watermarks
            self._bus.send(StartBackupCatchup((self.view_no, stashed_checkpoint_ends[-1])))
            self.caught_up_till_3pc((self.view_no, stashed_checkpoint_ends[-1]))

    def gc_before_new_view(self):
        self._reset_checkpoints()
        self._remove_stashed_checkpoints(till_3pc_key=(self.view_no, 0))

    def caught_up_till_3pc(self, caught_up_till_3pc):
        self._reset_checkpoints()
        self._remove_stashed_checkpoints(till_3pc_key=caught_up_till_3pc)
        self.update_watermark_from_3pc(caught_up_till_3pc)

    def catchup_clear_for_backup(self):
        self._reset_checkpoints()
        self._remove_stashed_checkpoints()
        self.set_watermarks(low_watermark=0,
                            high_watermark=sys.maxsize)

    def _add_to_checkpoint(self, ppSeqNo, digest, ledger_id, view_no):
        for (s, e) in self._checkpoint_state.keys():
            if s <= ppSeqNo <= e:
                state = self._checkpoint_state[s, e]  # type: CheckpointState
                state.digests.append(digest)
                state = updateNamedTuple(state, seqNo=ppSeqNo)
                self._checkpoint_state[s, e] = state
                break
        else:
            s, e = ppSeqNo, math.ceil(ppSeqNo / self._config.CHK_FREQ) * self._config.CHK_FREQ
            self._logger.debug("{} adding new checkpoint state for {}".format(self, (s, e)))
            state = CheckpointState(ppSeqNo, [digest, ], None, {}, False)
            self._checkpoint_state[s, e] = state

        if state.seqNo == e:
            if len(state.digests) == self._config.CHK_FREQ:
                self._do_checkpoint(state, s, e, ledger_id, view_no)
            self._process_stashed_checkpoints((s, e), view_no)

    # @measure_replica_time(MetricsName.SEND_CHECKPOINT_TIME,
    #                       MetricsName.BACKUP_SEND_CHECKPOINT_TIME)
    def _do_checkpoint(self, state, s, e, ledger_id, view_no):
        # TODO CheckpointState/Checkpoint is not a namedtuple anymore
        # 1. check if updateNamedTuple works for the new message type
        # 2. choose another name
        state = updateNamedTuple(state,
                                 digest=sha256(
                                     serialize_msg_for_signing(
                                         state.digests)
                                 ).hexdigest(),
                                 digests=[])
        self._checkpoint_state[s, e] = state
        self._logger.info("{} sending Checkpoint {} view {} checkpointState digest {}. Ledger {} "
                          "txn root hash {}. Committed state root hash {} Uncommitted state root hash {}".
                          format(self, (s, e), view_no, state.digest, ledger_id,
                                 self._db_manager.get_txn_root_hash(ledger_id),
                                 self._db_manager.get_state_root_hash(ledger_id,
                                                                      committed=True),
                                 self._db_manager.get_state_root_hash(ledger_id,
                                                                      committed=False)))
        checkpoint = Checkpoint(self._data.inst_id, view_no, s, e, state.digest)
        self._network.send(checkpoint)
        self._data.checkpoints.append(checkpoint)

    def _mark_checkpoint_stable(self, seqNo):
        previousCheckpoints = []
        for (s, e), state in self._checkpoint_state.items():
            if e == seqNo:
                # TODO CheckpointState/Checkpoint is not a namedtuple anymore
                # 1. check if updateNamedTuple works for the new message type
                # 2. choose another name
                state = updateNamedTuple(state, isStable=True)
                self._checkpoint_state[s, e] = state
                self._set_stable_checkpoint(e)
                break
            else:
                previousCheckpoints.append((s, e))
        else:
            self._logger.debug("{} could not find {} in checkpoints".format(self, seqNo))
            return
        self.set_watermarks(low_watermark=seqNo)
        for k in previousCheckpoints:
            self._logger.trace("{} removing previous checkpoint {}".format(self, k))
            self._checkpoint_state.pop(k)
        self._remove_stashed_checkpoints(till_3pc_key=(self.view_no, seqNo))
        self._bus.send(Cleanup((self.view_no, seqNo)))  # call OrderingService.l_gc()
        self._logger.info("{} marked stable checkpoint {}".format(self, (s, e)))

    def _check_if_checkpoint_stable(self, key: Tuple[int, int]):
        ckState = self._checkpoint_state[key]
        if self._data.quorums.checkpoint.is_reached(len(ckState.receivedDigests)):
            self._mark_checkpoint_stable(ckState.seqNo)
            return True
        else:
            self._logger.debug('{} has state.receivedDigests as {}'.format(
                self, ckState.receivedDigests.keys()))
            return False

    def _stash_checkpoint(self, ck: Checkpoint, sender: str):
        self._logger.debug('{} stashing {} from {}'.format(self, ck, sender))
        seqNoStart, seqNoEnd = ck.seqNoStart, ck.seqNoEnd
        if ck.viewNo not in self._stashed_recvd_checkpoints:
            self._stashed_recvd_checkpoints[ck.viewNo] = {}
        stashed_for_view = self._stashed_recvd_checkpoints[ck.viewNo]
        if (seqNoStart, seqNoEnd) not in stashed_for_view:
            stashed_for_view[seqNoStart, seqNoEnd] = {}
        stashed_for_view[seqNoStart, seqNoEnd][sender] = ck

    def _stashed_checkpoints_with_quorum(self):
        end_pp_seq_numbers = []
        quorum = self._data.quorums.checkpoint
        for (_, seq_no_end), senders in self._stashed_recvd_checkpoints.get(
                self.view_no, {}).items():
            if quorum.is_reached(len(senders)):
                end_pp_seq_numbers.append(seq_no_end)
        return sorted(end_pp_seq_numbers)

    def _process_stashed_checkpoints(self, key, view_no):
        # Remove all checkpoints from previous views if any
        self._remove_stashed_checkpoints(till_3pc_key=(self.view_no, 0))

        if key not in self._stashed_recvd_checkpoints.get(view_no, {}):
            self._logger.trace("{} have no stashed checkpoints for {}")
            return

        # Get a snapshot of all the senders of stashed checkpoints for `key`
        senders = list(self._stashed_recvd_checkpoints[view_no][key].keys())
        total_processed = 0
        consumed = 0

        for sender in senders:
            # Check if the checkpoint from `sender` is still in
            # `stashed_recvd_checkpoints` because it might be removed from there
            # in case own checkpoint was stabilized when we were processing
            # stashed checkpoints from previous senders in this loop
            if view_no in self._stashed_recvd_checkpoints \
                    and key in self._stashed_recvd_checkpoints[view_no] \
                    and sender in self._stashed_recvd_checkpoints[view_no][key]:
                if self.process_checkpoint(
                        self._stashed_recvd_checkpoints[view_no][key].pop(sender),
                        sender):
                    consumed += 1
                # Note that if `process_checkpoint` returned False then the
                # checkpoint from `sender` was re-stashed back to
                # `stashed_recvd_checkpoints`
                total_processed += 1

        # If we have consumed stashed checkpoints for `key` from all the
        # senders then remove entries which have become empty
        if view_no in self._stashed_recvd_checkpoints \
                and key in self._stashed_recvd_checkpoints[view_no] \
                and len(self._stashed_recvd_checkpoints[view_no][key]) == 0:
            del self._stashed_recvd_checkpoints[view_no][key]
            if len(self._stashed_recvd_checkpoints[view_no]) == 0:
                del self._stashed_recvd_checkpoints[view_no]

        restashed = total_processed - consumed
        self._logger.info('{} processed {} stashed checkpoints for {}, '
                          '{} of them were stashed again'.
                          format(self, total_processed, key, restashed))

        return total_processed

    def reset_watermarks_before_new_view(self):
        # Reset any previous view watermarks since for view change to
        # successfully complete, the node must have reached the same state
        # as other nodes
        self.set_watermarks(low_watermark=0)

    def should_reset_watermarks_before_new_view(self):
        if self.view_no <= 0:
            return False
        if self.last_ordered_3pc[0] == self.view_no and self.last_ordered_3pc[1] > 0:
            return False
        return True

    def set_watermarks(self, low_watermark: int, high_watermark: int = None):
        self._data.low_watermark = low_watermark
        if high_watermark is None:
            self._data.high_watermark = self._data.low_watermark + self._config.LOG_SIZE \
                if high_watermark is None else \
                high_watermark

        self._logger.info('{} set watermarks as {} {}'.format(self,
                                                              self._data.low_watermark,
                                                              self._data.high_watermark))
        self._old_stasher.unstash_watermarks()

    def update_watermark_from_3pc(self, last_ordered_3pc=None):
        if (self.last_ordered_3pc is not None) and (self.last_ordered_3pc[0] == self.view_no):
            self._logger.info("update_watermark_from_3pc to {}".format(self.last_ordered_3pc))
            low_watermark = self.last_ordered_3pc[1] if last_ordered_3pc is None \
                else last_ordered_3pc[1]
            self.set_watermarks(low_watermark)
        else:
            self._logger.info("try to update_watermark_from_3pc but last_ordered_3pc is None")

    def _remove_stashed_checkpoints(self, till_3pc_key=None):
        """
        Remove stashed received checkpoints up to `till_3pc_key` if provided,
        otherwise remove all stashed received checkpoints
        """
        if till_3pc_key is None:
            self._stashed_recvd_checkpoints.clear()
            self._logger.info('{} removing all stashed checkpoints'.format(self))
            return

        for view_no in list(self._stashed_recvd_checkpoints.keys()):

            if view_no < till_3pc_key[0]:
                self._logger.info('{} removing stashed checkpoints for view {}'.format(self, view_no))
                del self._stashed_recvd_checkpoints[view_no]

            elif view_no == till_3pc_key[0]:
                for (s, e) in list(self._stashed_recvd_checkpoints[view_no].keys()):
                    if e <= till_3pc_key[1]:
                        self._logger.info('{} removing stashed checkpoints: '
                                          'viewNo={}, seqNoStart={}, seqNoEnd={}'.
                                          format(self, view_no, s, e))
                        del self._stashed_recvd_checkpoints[view_no][(s, e)]
                if len(self._stashed_recvd_checkpoints[view_no]) == 0:
                    del self._stashed_recvd_checkpoints[view_no]

    def _reset_checkpoints(self):
        # That function most probably redundant in PBFT approach,
        # because according to paper, checkpoints cleared only when next stabilized.
        # Avoid using it while implement other services.
        self._checkpoint_state.clear()
        self._data.checkpoints.clear()
        # TODO: change to = 1 in ViewChangeService integration.
        self._data.stable_checkpoint = 0

    def _set_stable_checkpoint(self, end_seq_no):
        if not list(self._data.checkpoints.irange_key(end_seq_no, end_seq_no)):
            raise LogicError('Stable checkpoint must be in checkpoints')
        self._data.stable_checkpoint = end_seq_no

        self._data.checkpoints = \
            SortedListWithKey([c for c in self._data.checkpoints if c.seqNoEnd >= end_seq_no],
                              key=lambda checkpoint: checkpoint.seqNoEnd)

    # TODO: move to OrderingService as a handler for Cleanup messages
    # def _clear_batch_till_seq_no(self, seq_no):
    #     self._data.preprepared = [pp for pp in self._data.preprepared if pp.ppSeqNo >= seq_no]
    #     self._data.prepared = [p for p in self._data.prepared if p.ppSeqNo >= seq_no]
