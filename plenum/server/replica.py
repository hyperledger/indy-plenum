import time
from collections import deque, OrderedDict, defaultdict
from enum import unique, IntEnum
from functools import partial
from hashlib import sha256
from typing import List, Dict, Optional, Any, Set, Tuple, Callable, Iterable
import itertools

import math

import sys

import functools

from common.exceptions import LogicError, PlenumValueError
from common.serializers.serialization import serialize_msg_for_signing, state_roots_serializer, \
    invalid_index_serializer
from crypto.bls.bls_bft_replica import BlsBftReplica
from orderedset import OrderedSet

from plenum.common.config_util import getConfig
from plenum.common.constants import THREE_PC_PREFIX, PREPREPARE, PREPARE, \
    ReplicaHooks, DOMAIN_LEDGER_ID, COMMIT, POOL_LEDGER_ID, AUDIT_LEDGER_ID, AUDIT_TXN_PP_SEQ_NO, AUDIT_TXN_VIEW_NO, \
    AUDIT_TXN_PRIMARIES, TS_LABEL
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.exceptions import SuspiciousNode, \
    InvalidClientMessageException, UnknownIdentifier, SuspiciousPrePrepare
from plenum.common.hook_manager import HookManager
from plenum.common.ledger import Ledger
from plenum.common.message_processor import MessageProcessor
from plenum.common.messages.internal_messages import PrimariesBatchNeeded, \
    CurrentPrimaries, \
    NeedBackupCatchup, NeedMasterCatchup, CheckpointStabilized, RaisedSuspicion
from plenum.common.messages.message_base import MessageBase
from plenum.common.messages.node_messages import Reject, Ordered, \
    PrePrepare, Prepare, Commit, Checkpoint, CheckpointState, ThreePhaseMsg, ThreePhaseKey
from plenum.common.metrics_collector import NullMetricsCollector, MetricsCollector, MetricsName
from plenum.common.request import Request, ReqKey
from plenum.common.router import Subscription
from plenum.common.stashing_router import StashingRouter
from plenum.common.util import updateNamedTuple, compare_3PC_keys
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData, preprepare_to_batch_id
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.models import Commits, Prepares
from plenum.server.replica_freshness_checker import FreshnessChecker
from plenum.server.replica_helper import replica_batch_digest, TPCStat
from plenum.server.replica_stasher import ReplicaStasher
from plenum.server.replica_validator import ReplicaValidator
from plenum.server.replica_validator_enums import STASH_VIEW, STASH_WATERMARKS, STASH_CATCH_UP
from plenum.server.router import Router
from plenum.server.replica_helper import ConsensusDataHelper
from sortedcontainers import SortedList, SortedListWithKey
from stp_core.common.log import getlogger

import plenum.server.node

LOG_TAGS = {
    'PREPREPARE': {"tags": ["node-preprepare"]},
    'PREPARE': {"tags": ["node-prepare"]},
    'COMMIT': {"tags": ["node-commit"]},
    'ORDERED': {"tags": ["node-ordered"]}
}


class Replica3PRouter(Router):
    def __init__(self, replica, *args, **kwargs):
        self.replica = replica
        super().__init__(*args, *kwargs)

    # noinspection PyCallingNonCallable
    def handleSync(self, msg: Any) -> Any:
        try:
            super().handleSync(msg)
        except SuspiciousNode as ex:
            self.replica.report_suspicious_node(ex)


def measure_replica_time(master_name: MetricsName, backup_name: MetricsName):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            metrics = self.metrics
            if self.isMaster:
                with metrics.measure_time(master_name):
                    return f(self, *args, **kwargs)
            else:
                with metrics.measure_time(backup_name):
                    return f(self, *args, **kwargs)

        return wrapper

    return decorator


class Replica(HasActionQueue, MessageProcessor, HookManager):
    STASHED_CHECKPOINTS_BEFORE_CATCHUP = 1
    HAS_NO_PRIMARY_WARN_THRESCHOLD = 10

    def __init__(self, node: 'plenum.server.node.Node', instId: int,
                 config=None,
                 isMaster: bool = False,
                 bls_bft_replica: BlsBftReplica = None,
                 metrics: MetricsCollector = NullMetricsCollector(),
                 get_current_time=None,
                 get_time_for_3pc_batch=None):
        """
        Create a new replica.

        :param node: Node on which this replica is located
        :param instId: the id of the protocol instance the replica belongs to
        :param isMaster: is this a replica of the master protocol instance
        """
        HasActionQueue.__init__(self)
        self.get_current_time = get_current_time or time.perf_counter
        self.get_time_for_3pc_batch = get_time_for_3pc_batch or node.utc_epoch
        # self.stats = Stats(TPCStat)
        self.config = config or getConfig()
        self.metrics = metrics
        self.node = node
        self.instId = instId
        self.name = self.generateName(node.name, self.instId)
        self.logger = getlogger(self.name)
        self.validator = ReplicaValidator(self)
        self.stasher = self._init_replica_stasher()

        self.outBox = deque()
        """
        This queue is used by the replica to send messages to its node. Replica
        puts messages that are consumed by its node
        """

        self.inBox = deque()
        """
        This queue is used by the replica to receive messages from its node.
        Node puts messages that are consumed by the replica
        """

        self.inBoxStash = deque()
        """
        If messages need to go back on the queue, they go here temporarily and
        are put back on the queue on a state change
        """

        self._is_master = isMaster

        # Dictionary to keep track of the which replica was primary during each
        # view. Key is the view no and value is the name of the primary
        # replica during that view
        self.primaryNames = OrderedDict()  # type: OrderedDict[int, str]

        # Flag being used for preterm exit from the loop in the method
        # `processStashedMsgsForNewWaterMarks`. See that method for details.
        self.consumedAllStashedMsgs = True

        self._freshness_checker = FreshnessChecker(freshness_timeout=self.config.STATE_FRESHNESS_UPDATE_INTERVAL)

        self._bls_bft_replica = bls_bft_replica
        self._state_root_serializer = state_roots_serializer

        # Did we log a message about getting request while absence of primary
        self.warned_no_primary = False

        HookManager.__init__(self, ReplicaHooks.get_all_vals())

        self._consensus_data = ConsensusSharedData(self.name,
                                                   self.node.get_validators(),
                                                   self.instId,
                                                   self.isMaster)

        self._consensus_data_helper = ConsensusDataHelper(self._consensus_data)
        self._external_bus = ExternalBus(send_handler=self.send)
        self._subscription = Subscription()
        self._bootstrap_consensus_data()
        self._subscribe_to_external_msgs()
        self._subscribe_to_internal_msgs()
        self._checkpointer = self._init_checkpoint_service()
        self._ordering_service = self._init_ordering_service()
        for ledger_id in self.ledger_ids:
            self.register_ledger(ledger_id)

    def cleanup(self):
        # Aggregate all the currently forwarded requests
        req_keys = set()
        for msg in self.inBox:
            if isinstance(msg, ReqKey):
                req_keys.add(msg.digest)
        for req_queue in self._ordering_service.requestQueues.values():
            for req_key in req_queue:
                req_keys.add(req_key)
        for pp in self._ordering_service.sentPrePrepares.values():
            for req_key in pp.reqIdr:
                req_keys.add(req_key)
        for pp in self._ordering_service.prePrepares.values():
            for req_key in pp.reqIdr:
                req_keys.add(req_key)

        for req_key in req_keys:
            if req_key in self.requests:
                self.requests.ordered_by_replica(req_key)
                self.requests.free(req_key)

        self._ordering_service.cleanup()
        self._checkpointer.cleanup()
        self._subscription.unsubscribe_all()
        self.stasher.unsubscribe_from_all()

    @property
    def first_batch_after_catchup(self):
        # Defines if there was a batch after last catchup
        return self._ordering_service.first_batch_after_catchup

    @property
    def external_bus(self):
        # Defines if there was a batch after last catchup
        return self._external_bus

    @property
    def requested_pre_prepares(self):
        return self._ordering_service.requested_pre_prepares

    @property
    def requested_prepares(self):
        return self._ordering_service.requested_prepares

    @property
    def isMaster(self):
        return self._is_master

    @isMaster.setter
    def isMaster(self, value):
        self._is_master = value
        self._consensus_data.is_master = value

    @property
    def requested_commits(self):
        return self._ordering_service.requested_commits

    def _bootstrap_consensus_data(self):
        self._consensus_data.requests = self.requests
        self._consensus_data.low_watermark = self.h
        self._consensus_data.high_watermark = self.H
        self._consensus_data.node_mode = self.node.mode
        self._consensus_data.primaries_batch_needed = self.node.primaries_batch_needed
        self._consensus_data.quorums = self.quorums

    def _primaries_list_msg(self, msg: CurrentPrimaries):
        self._consensus_data.primaries = msg.primaries

    def _primaries_batch_needed_msg(self, msg: PrimariesBatchNeeded):
        self._consensus_data.primaries_batch_needed = msg.pbn

    def _subscribe_to_external_msgs(self):
        # self._subscription.subscribe(self._external_bus, ReqKey, self.readyFor3PC)
        pass

    def _subscribe_to_internal_msgs(self):
        self._subscription.subscribe(self.node.internal_bus, PrimariesBatchNeeded, self._primaries_batch_needed_msg)
        self._subscription.subscribe(self.node.internal_bus, CurrentPrimaries, self._primaries_list_msg)
        self._subscription.subscribe(self.node.internal_bus, Ordered, self._send_ordered)
        self._subscription.subscribe(self.node.internal_bus, NeedBackupCatchup, self._caught_up_backup)
        self._subscription.subscribe(self.node.internal_bus, CheckpointStabilized, self._cleanup_process)
        self._subscription.subscribe(self.node.internal_bus, ReqKey, self.readyFor3PC)
        self._subscription.subscribe(self.node.internal_bus, RaisedSuspicion, self._process_suspicious_node)

    def register_ledger(self, ledger_id):
        # Using ordered set since after ordering each PRE-PREPARE,
        # the request key is removed, so fast lookup and removal of
        # request key is needed. Need the collection to be ordered since
        # the request key needs to be removed once its ordered
        if ledger_id not in self._ordering_service.requestQueues:
            self._ordering_service.requestQueues[ledger_id] = OrderedSet()
        if ledger_id != AUDIT_LEDGER_ID:
            self._freshness_checker.register_ledger(ledger_id=ledger_id,
                                                    initial_time=self.get_time_for_3pc_batch())

    def get_sent_preprepare(self, viewNo, ppSeqNo):
        return self._ordering_service.get_sent_preprepare(viewNo, ppSeqNo)

    def get_sent_prepare(self, viewNo, ppSeqNo):
        return self._ordering_service.get_sent_prepare(viewNo, ppSeqNo)

    def get_sent_commit(self, viewNo, ppSeqNo):
        return self._ordering_service.get_sent_commit(viewNo, ppSeqNo)

    @property
    def last_prepared_before_view_change(self):
        return self._consensus_data.legacy_last_prepared_before_view_change

    @last_prepared_before_view_change.setter
    def last_prepared_before_view_change(self, lst):
        self._consensus_data.legacy_last_prepared_before_view_change = lst

    @property
    def h(self) -> int:
        return self._consensus_data.low_watermark

    @property
    def H(self) -> int:
        return self._consensus_data.high_watermark

    @property
    def last_ordered_3pc(self) -> tuple:
        return self._consensus_data.last_ordered_3pc

    @last_ordered_3pc.setter
    def last_ordered_3pc(self, key3PC):
        self._consensus_data.last_ordered_3pc = key3PC
        self.logger.info('{} set last ordered as {}'.format(self, key3PC))

    @property
    def lastPrePrepareSeqNo(self):
        return self._ordering_service._lastPrePrepareSeqNo

    @lastPrePrepareSeqNo.setter
    def lastPrePrepareSeqNo(self, n):
        """
        This will _lastPrePrepareSeqNo to values greater than its previous
        values else it will not. To forcefully override as in case of `revert`,
        directly set `self._lastPrePrepareSeqNo`
        """
        if n > self._ordering_service._lastPrePrepareSeqNo:
            # ToDo: need to pass it into ordering service through ConsensusDataProvider
            self._ordering_service._lastPrePrepareSeqNo = n
        else:
            self.logger.debug(
                '{} cannot set lastPrePrepareSeqNo to {} as its '
                'already {}'.format(
                    self, n, self.lastPrePrepareSeqNo))

    @property
    def requests(self):
        return self.node.requests

    @property
    def ledger_ids(self):
        return self.node.ledger_ids

    @property
    def quorums(self):
        return self.node.quorums

    @property
    def utc_epoch(self):
        return self.node.utc_epoch()

    @staticmethod
    def generateName(nodeName: str, instId: int):
        """
        Create and return the name for a replica using its nodeName and
        instanceId.
         Ex: Alpha:1
        """

        if isinstance(nodeName, str):
            # Because sometimes it is bytes (why?)
            if ":" in nodeName:
                # Because in some cases (for requested messages) it
                # already has ':'. This should be fixed.
                return nodeName
        return "{}:{}".format(nodeName, instId)

    @staticmethod
    def getNodeName(replicaName: str):
        return replicaName.split(":")[0]

    @property
    def isPrimary(self):
        """
        Is this node primary?

        :return: True if this node is primary, False if not, None if primary status not known
        """
        return self._consensus_data.is_primary

    @property
    def hasPrimary(self):
        return self.primaryName is not None

    @property
    def primaryName(self):
        """
        Name of the primary replica of this replica's instance

        :return: Returns name if primary is known, None otherwise
        """
        return self._consensus_data.primary_name

    @primaryName.setter
    def primaryName(self, value: Optional[str]) -> None:
        """
        Set the value of isPrimary.

        :param value: the value to set isPrimary to
        """
        if value is not None:
            self.warned_no_primary = False
        self.primaryNames[self.viewNo] = value
        self.compact_primary_names()
        if value != self._consensus_data.primary_name:
            self._consensus_data.primary_name = value
            self.logger.info("{} setting primaryName for view no {} to: {}".
                             format(self, self.viewNo, value))
            if value is None:
                # Since the GC needs to happen after a primary has been
                # decided.
                return
            self._gc_before_new_view()
            if self._checkpointer.should_reset_watermarks_before_new_view():
                self._checkpointer.reset_watermarks_before_new_view()
                self._ordering_service._lastPrePrepareSeqNo = 0

    def compact_primary_names(self):
        min_allowed_view_no = self.viewNo - 1
        views_to_remove = []
        for view_no in self.primaryNames:
            if view_no >= min_allowed_view_no:
                break
            views_to_remove.append(view_no)
        for view_no in views_to_remove:
            self.primaryNames.pop(view_no)

    def primaryChanged(self, primaryName):
        self._ordering_service.batches.clear()
        if self.isMaster:
            # Since there is no temporary state data structure and state root
            # is explicitly set to correct value
            for lid in self.ledger_ids:
                try:
                    ledger = self.node.getLedger(lid)
                except KeyError:
                    continue
                ledger.reset_uncommitted()

        self.primaryName = primaryName
        self._setup_for_non_master_after_view_change(self.viewNo)

    def on_view_change_start(self):
        if self.isMaster:
            lst = self._ordering_service.l_last_prepared_certificate_in_view()
            self._consensus_data.legacy_last_prepared_before_view_change = lst
            self.logger.info('{} setting last prepared for master to {}'.format(self, lst))

    def on_view_change_done(self):
        if self.isMaster:
            self.last_prepared_before_view_change = None
        self.stasher.process_all_stashed(STASH_VIEW)

    def clear_requests_and_fix_last_ordered(self):
        if self.isMaster:
            return
        reqs_for_remove = []
        for req in self.requests.values():
            ledger_id, seq_no = self.node.seqNoDB.get_by_payload_digest(req.request.payload_digest)
            if seq_no is not None:
                reqs_for_remove.append((req.request.digest, ledger_id, seq_no))
        for key, ledger_id, seq_no in reqs_for_remove:
            self.requests.ordered_by_replica(key)
            self.requests.free(key)
            self._ordering_service.requestQueues[int(ledger_id)].discard(key)
        master_last_ordered_3pc = self.node.master_replica.last_ordered_3pc
        if compare_3PC_keys(master_last_ordered_3pc, self.last_ordered_3pc) < 0 \
                and self.isPrimary is False:
            self.last_ordered_3pc = master_last_ordered_3pc

    def on_propagate_primary_done(self):
        if self.isMaster:
            # if this is a Primary that is re-connected (that is view change is not actually changed,
            # we just propagate it, then make sure that we did't break the sequence
            # of ppSeqNo
            self._checkpointer.update_watermark_from_3pc()
            if self.isPrimary and (self.last_ordered_3pc[0] == self.viewNo):
                self.lastPrePrepareSeqNo = self.last_ordered_3pc[1]
        elif not self.isPrimary:
            self._checkpointer.set_watermarks(low_watermark=0,
                                              high_watermark=sys.maxsize)

    def get_lowest_probable_prepared_certificate_in_view(
            self, view_no) -> Optional[int]:
        """
        Return lowest pp_seq_no of the view for which can be prepared but
        choose from unprocessed PRE-PREPAREs and PREPAREs.
        """
        # TODO: Naive implementation, dont need to iterate over the complete
        # data structures, fix this later
        seq_no_pp = SortedList()  # pp_seq_no of PRE-PREPAREs
        # pp_seq_no of PREPAREs with count of PREPAREs for each
        seq_no_p = set()

        for (v, p) in self._ordering_service.prePreparesPendingPrevPP:
            if v == view_no:
                seq_no_pp.add(p)
            if v > view_no:
                break

        for (v, p), pr in self._ordering_service.preparesWaitingForPrePrepare.items():
            if v == view_no and len(pr) >= self.quorums.prepare.value:
                seq_no_p.add(p)

        for n in seq_no_pp:
            if n in seq_no_p:
                return n
        return None

    def _setup_for_non_master_after_view_change(self, current_view):
        if not self.isMaster:
            for v in list(self.stashed_out_of_order_commits.keys()):
                if v < current_view:
                    self.stashed_out_of_order_commits.pop(v)

    def is_primary_in_view(self, viewNo: int) -> Optional[bool]:
        """
        Return whether this replica was primary in the given view
        """
        if viewNo not in self.primaryNames:
            return False
        return self.primaryNames[viewNo] == self.name

    def isMsgForCurrentView(self, msg):
        """
        Return whether this request's view number is equal to the current view
        number of this replica.
        """
        viewNo = getattr(msg, "viewNo", None)
        return viewNo == self.viewNo

    def isPrimaryForMsg(self, msg) -> Optional[bool]:
        """
        Return whether this replica is primary if the request's view number is
        equal this replica's view number and primary has been selected for
        the current view.
        Return None otherwise.
        :param msg: message
        """
        return self.isPrimary if self.isMsgForCurrentView(msg) \
            else self.is_primary_in_view(msg.viewNo)

    def isMsgFromPrimary(self, msg, sender: str) -> bool:
        """
        Return whether this message was from primary replica
        :param msg:
        :param sender:
        :return:
        """
        if self.isMsgForCurrentView(msg):
            return self.primaryName == sender
        try:
            return self.primaryNames[msg.viewNo] == sender
        except KeyError:
            return False

    def __repr__(self):
        return self.name

    @property
    def f(self) -> int:
        """
        Return the number of Byzantine Failures that can be tolerated by this
        system. Equal to (N - 1)/3, where N is the number of nodes in the
        system.
        """
        return self.node.f

    @property
    def viewNo(self):
        """
        Return the current view number of this replica.
        """
        return self._consensus_data.view_no

    @property
    def stashed_out_of_order_commits(self):
        # Commits which are not being ordered since commits with lower
        # sequence numbers have not been ordered yet. Key is the
        # viewNo and value a map of pre-prepare sequence number to commit
        # type: Dict[int,Dict[int,Commit]]
        return self._ordering_service.stashed_out_of_order_commits

    def send_3pc_batch(self):
        return self._ordering_service.l_send_3pc_batch()

    @staticmethod
    def batchDigest(reqs):
        return replica_batch_digest(reqs)

    def readyFor3PC(self, key: ReqKey):
        try:
            fin_req = self.requests[key.digest].finalised
        except KeyError:
            # Means that this request is outdated and is dropped from the main requests queue
            self.logger.debug('{} reports request {} is ready for 3PC but it has been dropped '
                              'from requests queue, ignore this request'.format(self, key))
            return
        queue = self._ordering_service.requestQueues[self.node.ledger_id_for_request(fin_req)]
        queue.add(key.digest)
        if not self.hasPrimary and len(queue) >= self.HAS_NO_PRIMARY_WARN_THRESCHOLD and not self.warned_no_primary:
            self.logger.warning('{} is getting requests but still does not have '
                                'a primary so the replica will not process the request '
                                'until a primary is chosen'.format(self))
            self.warned_no_primary = True

    @measure_replica_time(MetricsName.SERVICE_REPLICA_QUEUES_TIME,
                          MetricsName.SERVICE_BACKUP_REPLICAS_QUEUES_TIME)
    def serviceQueues(self, limit=None):
        """
        Process `limit` number of messages in the inBox.

        :param limit: the maximum number of messages to process
        :return: the number of messages successfully processed
        """
        # TODO should handle SuspiciousNode here
        r = self.dequeue_pre_prepares()
        # r += self.inBoxRouter.handleAllSync(self.inBox, limit)
        r += self._handle_external_messages(self.inBox, limit)
        r += self.send_3pc_batch()
        r += self._serviceActions()
        return r
        # Messages that can be processed right now needs to be added back to the
        # queue. They might be able to be processed later

    def _handle_external_messages(self, deq: deque, limit=None) -> int:
        """
        Synchronously handle all items in a deque.

        :param deq: a deque of items to be handled by this router
        :param limit: the number of items in the deque to the handled
        :return: the number of items handled successfully
        """
        count = 0
        while deq and (not limit or count < limit):
            count += 1
            msg = deq.popleft()
            external_msg, sender = msg if len(msg) == 2 else (msg, None)
            sender = self.generateName(sender, self.instId)
            self._external_bus.process_incoming(external_msg, sender)
        return count

    def _gc_before_new_view(self):
        # Trigger GC for all batches of old view
        # Clear any checkpoints, since they are valid only in a view
        # ToDo: Need to send a cmd like ViewChangeStart into internal bus
        # self._gc(self.last_ordered_3pc)
        self._ordering_service.l_gc(self.last_ordered_3pc)
        self._checkpointer.gc_before_new_view()
        # ToDo: get rid of directly calling
        self._ordering_service._clear_prev_view_pre_prepares()
        # self._clear_prev_view_pre_prepares()

    def has_already_ordered(self, view_no, pp_seq_no):
        return compare_3PC_keys((view_no, pp_seq_no),
                                self.last_ordered_3pc) >= 0

    def dequeue_pre_prepares(self):
        return self._ordering_service.l_dequeue_pre_prepares()

    def getDigestFor3PhaseKey(self, key: ThreePhaseKey) -> Optional[str]:
        reqKey = self.getReqKeyFrom3PhaseKey(key)
        digest = self.requests.digest(reqKey)
        if not digest:
            self.logger.debug("{} could not find digest in sent or received "
                              "PRE-PREPAREs or PREPAREs for 3 phase key {} and req "
                              "key {}".format(self, key, reqKey))
            return None
        else:
            return digest

    def getReqKeyFrom3PhaseKey(self, key: ThreePhaseKey):
        reqKey = None
        if key in self.sentPrePrepares:
            reqKey = self.sentPrePrepares[key][0]
        elif key in self.prePrepares:
            reqKey = self.prePrepares[key][0]
        elif key in self.prepares:
            reqKey = self.prepares[key][0]
        else:
            self.logger.debug("Could not find request key for 3 phase key {}".format(key))
        return reqKey

    def _process_requested_three_phase_msg(self, msg: object,
                                           sender: List[str],
                                           stash: Dict[Tuple[int, int], Optional[Tuple[str, str, str]]],
                                           get_saved: Optional[Callable[[int, int], Optional[MessageBase]]] = None):
        if msg is None:
            self.logger.debug('{} received null from {}'.format(self, sender))
            return
        key = (msg.viewNo, msg.ppSeqNo)
        self.logger.debug('{} received requested msg ({}) from {}'.format(self, key, sender))

        if key not in stash:
            self.logger.debug('{} had either not requested this msg or already '
                              'received the msg for {}'.format(self, key))
            return
        if self.has_already_ordered(*key):
            self.logger.debug(
                '{} has already ordered msg ({})'.format(self, key))
            return
        if get_saved and get_saved(*key):
            self.logger.debug(
                '{} has already received msg ({})'.format(self, key))
            return
        # There still might be stashed msg but not checking that
        # it is expensive, also reception of msgs is idempotent
        stashed_data = stash[key]
        curr_data = (msg.digest, msg.stateRootHash, msg.txnRootHash) \
            if isinstance(msg, PrePrepare) or isinstance(msg, Prepare) \
            else None
        if stashed_data is None or curr_data == stashed_data:
            return self._external_bus.process_incoming(msg, sender)

        self.discard(msg, reason='{} does not have expected state {}'.
                     format(THREE_PC_PREFIX, stashed_data),
                     logMethod=self.logger.warning)

    def process_requested_pre_prepare(self, pp: PrePrepare, sender: str):
        return self._process_requested_three_phase_msg(pp, sender, self.requested_pre_prepares,
                                                       self._ordering_service.l_getPrePrepare)

    def process_requested_prepare(self, prepare: Prepare, sender: str):
        return self._process_requested_three_phase_msg(prepare, sender, self.requested_prepares)

    def process_requested_commit(self, commit: Commit, sender: str):
        return self._process_requested_three_phase_msg(commit, sender, self.requested_commits)

    def send(self, msg, to_nodes=None) -> None:
        """
        Send a message to the node on which this replica resides.

        :param stat:
        :param rid: remote id of one recipient (sends to all recipients if None)
        :param msg: the message to send
        """
        self.logger.trace("{} sending {}".format(self, msg.__class__.__name__),
                          extra={"cli": True, "tags": ['sending']})
        self.logger.trace("{} sending {}".format(self, msg))
        if to_nodes:
            self.node.sendToNodes(msg, names=to_nodes)
            return
        self.outBox.append(msg)

    def revert_unordered_batches(self):
        """
        Revert changes to ledger (uncommitted) and state made by any requests
        that have not been ordered.
        """
        return self._ordering_service.revert_unordered_batches()

    def on_catch_up_finished(self, last_caught_up_3PC=None, master_last_ordered_3PC=None):
        if master_last_ordered_3PC and last_caught_up_3PC and \
                compare_3PC_keys(master_last_ordered_3PC,
                                 last_caught_up_3PC) > 0:
            if self.isMaster:
                self._caught_up_till_3pc(last_caught_up_3PC)
            else:
                self._ordering_service.first_batch_after_catchup = True
                self._catchup_clear_for_backup()
        self.stasher.process_all_stashed(STASH_CATCH_UP)

    def discard_req_key(self, ledger_id, req_key):
        return self._ordering_service.l_discard_req_key(ledger_id, req_key)

    def _caught_up_backup(self, msg: NeedBackupCatchup):
        if self.instId != msg.inst_id:
            return
        self._caught_up_till_3pc(msg.caught_up_till_3pc)

    def _caught_up_till_3pc(self, last_caught_up_3PC):
        self._ordering_service._caught_up_till_3pc(last_caught_up_3PC)
        self._checkpointer.caught_up_till_3pc(last_caught_up_3PC)

    def _catchup_clear_for_backup(self):
        if not self.isPrimary:
            self.outBox.clear()
            self._checkpointer.catchup_clear_for_backup()
            self._ordering_service.catchup_clear_for_backup()

    def _remove_ordered_from_queue(self, last_caught_up_3PC=None):
        """
        Remove any Ordered that the replica might be sending to node which is
        less than or equal to `last_caught_up_3PC` if `last_caught_up_3PC` is
        passed else remove all ordered, needed in catchup
        """
        to_remove = []
        for i, msg in enumerate(self.outBox):
            if isinstance(msg, Ordered) and \
                    (not last_caught_up_3PC or
                     compare_3PC_keys((msg.viewNo, msg.ppSeqNo), last_caught_up_3PC) >= 0):
                to_remove.append(i)

        self.logger.trace('{} going to remove {} Ordered messages from outbox'.format(self, len(to_remove)))

        # Removing Ordered from queue but returning `Ordered` in order that
        # they should be processed.
        removed = []
        for i in reversed(to_remove):
            removed.insert(0, self.outBox[i])
            del self.outBox[i]
        return removed

    def _get_last_timestamp_from_state(self, ledger_id):
        if ledger_id == DOMAIN_LEDGER_ID:
            ts_store = self.node.db_manager.get_store(TS_LABEL)
            if ts_store:
                last_timestamp = ts_store.get_last_key()
                if last_timestamp:
                    last_timestamp = int(last_timestamp.decode())
                    self.logger.debug("Last ordered timestamp from store is : {}"
                                      "".format(last_timestamp))
                    return last_timestamp
        return None

    def get_ledgers_last_update_time(self) -> dict:
        if self._freshness_checker:
            return self._freshness_checker.get_last_update_time()

    def get_valid_req_ids_from_all_requests(self, reqs, invalid_indices):
        return [req.key for idx, req in enumerate(reqs) if idx not in invalid_indices]

    def report_suspicious_node(self, ex):
        if self.isMaster:
            self.node.reportSuspiciousNodeEx(ex)
        else:
            self.warn_suspicious_backup(ex.node, ex.reason, ex.code)

    def warn_suspicious_backup(self, nodeName, reason, code):
        self.logger.warning("backup replica {} raised suspicion on node {} for {}; suspicion code "
                            "is {}".format(self, nodeName, reason, code))

    def set_validators(self, validators):
        self._consensus_data.set_validators(validators)

    def set_view_no(self, view_no):
        self._consensus_data.view_no = view_no

    def set_view_change_status(self, legacy_vc_in_progress):
        self._consensus_data.legacy_vc_in_progress = legacy_vc_in_progress

    def set_mode(self, mode):
        self._consensus_data.node_mode = mode

    def update_connecteds(self, connecteds: dict):
        self._external_bus.update_connecteds(connecteds)

    def _init_checkpoint_service(self) -> CheckpointService:
        return CheckpointService(data=self._consensus_data,
                                 bus=self.node.internal_bus,
                                 network=self._external_bus,
                                 stasher=self.stasher,
                                 db_manager=self.node.db_manager,
                                 metrics=self.metrics)

    def _init_replica_stasher(self):
        return StashingRouter(self.config.REPLICA_STASH_LIMIT,
                              replica_unstash=self._add_to_inbox)

    def _cleanup_process(self, msg: CheckpointStabilized):
        if msg.inst_id != self.instId:
            return
        self._ordering_service.l_gc(msg.last_stable_3pc)

    def _process_suspicious_node(self, msg: RaisedSuspicion):
        if msg.inst_id != self.instId:
            return
        self.report_suspicious_node(msg.ex)

    def _send_ordered(self, msg: Ordered):
        if msg.instId != self.instId:
            return
        self.send(msg)

    def _init_ordering_service(self) -> OrderingService:
        return OrderingService(data=self._consensus_data,
                               timer=self.node.timer,
                               bus=self.node.internal_bus,
                               network=self._external_bus,
                               write_manager=self.node.write_manager,
                               bls_bft_replica=self._bls_bft_replica,
                               freshness_checker=self._freshness_checker,
                               get_current_time=self.get_current_time,
                               get_time_for_3pc_batch=self.get_time_for_3pc_batch,
                               stasher=self.stasher,
                               metrics=self.metrics)

    def _add_to_inbox(self, message):
        self.inBox.append(message)
