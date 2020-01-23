import itertools
import logging
import time
from _sha256 import sha256
from collections import defaultdict, OrderedDict, deque
from functools import partial
from typing import Tuple, List, Set, Optional, Dict, Iterable, Callable

from orderedset._orderedset import OrderedSet
from sortedcontainers import SortedList

from common.exceptions import PlenumValueError, LogicError
from common.serializers.serialization import state_roots_serializer, invalid_index_serializer, serialize_msg_for_signing
from crypto.bls.bls_bft_replica import BlsBftReplica
from plenum.common.config_util import getConfig
from plenum.common.constants import POOL_LEDGER_ID, SEQ_NO_DB_LABEL, AUDIT_LEDGER_ID, TXN_TYPE, \
    LAST_SENT_PP_STORE_LABEL, AUDIT_TXN_PP_SEQ_NO, AUDIT_TXN_VIEW_NO, AUDIT_TXN_PRIMARIES, AUDIT_TXN_DIGEST, \
    PREPREPARE, PREPARE, COMMIT, DOMAIN_LEDGER_ID, TS_LABEL, AUDIT_TXN_NODE_REG
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.exceptions import SuspiciousNode, InvalidClientMessageException, SuspiciousPrePrepare, \
    UnknownIdentifier
from plenum.common.ledger import Ledger
from plenum.common.messages.internal_messages import RequestPropagates, BackupSetupLastOrdered, \
    RaisedSuspicion, ViewChangeStarted, NewViewCheckpointsApplied, MissingMessage, CheckpointStabilized, \
    ReAppliedInNewView, NewViewAccepted, CatchupCheckpointsApplied, MasterReorderedAfterVC
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit, Reject, ThreePhaseKey, Ordered, \
    OldViewPrePrepareRequest, OldViewPrePrepareReply
from plenum.common.metrics_collector import MetricsName, MetricsCollector, NullMetricsCollector
from plenum.common.request import Request
from plenum.common.router import Subscription
from plenum.common.stashing_router import PROCESS
from plenum.common.timer import TimerService, RepeatingTimer
from plenum.common.txn_util import get_payload_digest, get_payload_data, get_seq_no, get_txn_time, get_digest
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys, updateNamedTuple, SortedDict, getMaxFailures, mostCommonElement, \
    get_utc_epoch, max_3PC_key
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.batch_id import BatchID
from plenum.server.consensus.metrics_decorator import measure_consensus_time
from plenum.server.consensus.ordering_service_msg_validator import OrderingServiceMsgValidator
from plenum.server.consensus.primary_selector import PrimariesSelector
from plenum.server.consensus.utils import replica_name_to_node_name, get_original_viewno, preprepare_to_batch_id
from plenum.server.replica_helper import PP_APPLY_REJECT_WRONG, PP_APPLY_WRONG_DIGEST, PP_APPLY_WRONG_STATE, \
    PP_APPLY_ROOT_HASH_MISMATCH, PP_APPLY_HOOK_ERROR, PP_SUB_SEQ_NO_WRONG, PP_NOT_FINAL, PP_APPLY_AUDIT_HASH_MISMATCH, \
    PP_REQUEST_ALREADY_ORDERED, PP_CHECK_NOT_FROM_PRIMARY, PP_CHECK_TO_PRIMARY, PP_CHECK_DUPLICATE, \
    PP_CHECK_INCORRECT_POOL_STATE_ROOT, PP_CHECK_OLD, PP_CHECK_REQUEST_NOT_FINALIZED, PP_CHECK_NOT_NEXT, \
    PP_CHECK_WRONG_TIME, Stats, OrderedTracker, TPCStat, generateName, PP_WRONG_PRIMARIES
from plenum.server.replica_freshness_checker import FreshnessChecker
from plenum.server.replica_helper import replica_batch_digest
from plenum.server.replica_validator_enums import STASH_VIEW_3PC, STASH_CATCH_UP, STASH_WAITING_FIRST_BATCH_IN_VIEW
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from plenum.server.suspicion_codes import Suspicions
from stp_core.common.log import getlogger

logger = getlogger()


class OrderingService:

    def __init__(self,
                 data: ConsensusSharedData,
                 timer: TimerService,
                 bus: InternalBus,
                 network: ExternalBus,
                 write_manager: WriteRequestManager,
                 bls_bft_replica: BlsBftReplica,
                 freshness_checker: FreshnessChecker,
                 stasher=None,
                 get_current_time: Optional[Callable[[], float]] = None,
                 get_time_for_3pc_batch: Optional[Callable[[], int]] = None,
                 metrics: MetricsCollector = NullMetricsCollector()):
        self.metrics = metrics
        self._data = data
        self._requests = self._data.requests
        self._timer = timer
        self._bus = bus
        self._network = network
        self._write_manager = write_manager
        self._name = self._data.name
        # TODO: We shouldn't use get_utc_epoch here, time needs to be under full control through TimerService
        self.get_time_for_3pc_batch = get_time_for_3pc_batch or get_utc_epoch
        # Flag which node set, when it have set new primaries and need to send batch
        self.primaries_batch_needed = False

        self._config = getConfig()
        # TODO: Change just to self._stasher = stasher
        self._stasher = stasher
        self._subscription = Subscription()
        self._validator = OrderingServiceMsgValidator(self._data)
        self.get_current_time = get_current_time or self._timer.get_current_time
        self._out_of_order_repeater = RepeatingTimer(self._timer,
                                                     self._config.PROCESS_STASHED_OUT_OF_ORDER_COMMITS_INTERVAL,
                                                     self._process_stashed_out_of_order_commits,
                                                     active=False)

        """
        Maps from legacy replica code
        """
        self._state_root_serializer = state_roots_serializer

        # Keeps a map of PRE-PREPAREs which did not satisfy timestamp
        # criteria, they can be accepted if >f PREPAREs are encountered.
        # This is emptied on view change. With each PRE-PREPARE, a flag is
        # stored which indicates whether there are sufficient acceptable
        # PREPAREs or not
        self.pre_prepares_stashed_for_incorrect_time = {}

        # Time of the last PRE-PREPARE which satisfied all validation rules
        # (time, digest, roots were all correct). This time is not to be
        # reverted even if the PRE-PREPAREs are not ordered. This implies that
        # the next primary would have seen all accepted PRE-PREPAREs or another
        # view change will happen
        self.last_accepted_pre_prepare_time = None

        # PRE-PREPAREs timestamps stored by non primary replica to check
        # obsolescence of incoming PrePrepares. Pre-prepares with the same
        # 3PC key are not merged since we need to keep incoming timestamps
        # for each new PP from every nodes separately.
        # Dictionary:
        #   key: Tuple[pp.viewNo, pp.seqNo]
        #   value: Dict[Tuple[PrePrepare, sender], timestamp]
        self.pre_prepare_tss = defaultdict(dict)

        # PRE-PREPAREs that are waiting to be processed but do not have the
        # corresponding request finalised. Happens when replica has not been
        # forwarded the request by the node but is getting 3 phase messages.
        # The value is a list since a malicious entry might send PRE-PREPARE
        # with a different digest and since we dont have the request finalised
        # yet, we store all PRE-PPREPAREs
        # type: List[Tuple[PrePrepare, str, Set[Tuple[str, int]]]]
        self.prePreparesPendingFinReqs = []

        # PrePrepares waiting for previous PrePrepares, key being tuple of view
        # number and pre-prepare sequence numbers and value being tuple of
        # PrePrepare and sender
        # TODO: Since pp_seq_no will start from 1 in each view, the comparator
        # of SortedDict needs to change
        self.prePreparesPendingPrevPP = SortedDict(lambda k: (k[0], k[1]))

        # PREPAREs that are stored by non primary replica for which it has not
        #  got any PRE-PREPARE. Dictionary that stores a tuple of view no and
        #  prepare sequence number as key and a deque of PREPAREs as value.
        # This deque is attempted to be flushed on receiving every
        # PRE-PREPARE request.
        self.preparesWaitingForPrePrepare = {}

        # Defines if there was a batch after last catchup
        self.first_batch_after_catchup = False

        self._lastPrePrepareSeqNo = self._data.low_watermark  # type: int

        # COMMITs that are stored for which there are no PRE-PREPARE or PREPARE
        # received
        self.commitsWaitingForPrepare = {}
        # type: Dict[Tuple[int, int], deque]

        # Dictionary of received PRE-PREPAREs. Key of dictionary is a 2
        # element tuple with elements viewNo, pre-prepare seqNo and value
        # is the received PRE-PREPARE
        self.prePrepares = SortedDict(lambda k: (k[0], k[1]))
        # type: Dict[Tuple[int, int], PrePrepare]

        # Dictionary to keep track of the which replica was primary during each
        # view. Key is the view no and value is the name of the primary
        # replica during that view
        self.primary_names = OrderedDict()  # type: OrderedDict[int, str]

        # Did we log a message about getting request while absence of primary
        self.warned_no_primary = False

        self.requestQueues = {}  # type: Dict[int, OrderedSet]

        self.stats = Stats(TPCStat)

        self.batches = OrderedDict()  # type: OrderedDict[Tuple[int, int]]

        self.l_bls_bft_replica = bls_bft_replica

        # Set of tuples to keep track of ordered requests. Each tuple is
        # (viewNo, ppSeqNo).
        self.ordered = OrderedTracker()

        self.lastBatchCreated = self.get_current_time()

        # Commits which are not being ordered since commits with lower
        # sequence numbers have not been ordered yet. Key is the
        # viewNo and value a map of pre-prepare sequence number to commit
        # type: Dict[int,Dict[int,Commit]]
        self.stashed_out_of_order_commits = {}

        self._freshness_checker = freshness_checker
        self._skip_send_3pc_ts = None

        self._subscription.subscribe(self._stasher, PrePrepare, self.process_preprepare)
        self._subscription.subscribe(self._stasher, Prepare, self.process_prepare)
        self._subscription.subscribe(self._stasher, Commit, self.process_commit)
        self._subscription.subscribe(self._stasher, NewViewCheckpointsApplied, self.process_new_view_checkpoints_applied)
        self._subscription.subscribe(self._stasher, OldViewPrePrepareRequest, self.process_old_view_preprepare_request)
        self._subscription.subscribe(self._stasher, OldViewPrePrepareReply, self.process_old_view_preprepare_reply)
        self._subscription.subscribe(self._bus, ViewChangeStarted, self.process_view_change_started)
        self._subscription.subscribe(self._bus, CheckpointStabilized, self.process_checkpoint_stabilized)
        self._subscription.subscribe(self._bus, NewViewAccepted, self.process_new_view_accepted)
        self._subscription.subscribe(self._bus, CatchupCheckpointsApplied, self.process_catchup_checkpoints_applied)

        # Dict to keep PrePrepares from old view to be re-ordered in the new view
        # key is (viewNo, ppSeqNo, ppDigest) tuple, and value is PrePrepare
        self.old_view_preprepares = {}

        # TODO: find a better place for calling this setter
        if self.is_master:
            self._write_manager.node_reg_handler.set_internal_bus(self._bus)

    def cleanup(self):
        self._subscription.unsubscribe_all()

    def __repr__(self):
        return self.name

    @measure_consensus_time(MetricsName.PROCESS_PREPARE_TIME,
                            MetricsName.BACKUP_PROCESS_PREPARE_TIME)
    def process_prepare(self, prepare: Prepare, sender: str):
        """
        Validate and process the PREPARE specified.
        If validation is successful, create a COMMIT and broadcast it.

        :param prepare: a PREPARE msg
        :param sender: name of the node that sent the PREPARE
        """
        result, reason = self._validate(prepare)
        if result != PROCESS:
            return result, reason

        key = (prepare.viewNo, prepare.ppSeqNo)
        logger.debug("{} received PREPARE{} from {}".format(self, key, sender))

        # TODO move this try/except up higher
        try:
            if self._validate_prepare(prepare, sender):
                self._add_to_prepares(prepare, sender)
                self.stats.inc(TPCStat.PrepareRcvd)
                logger.debug("{} processed incoming PREPARE {}".format(
                    self, (prepare.viewNo, prepare.ppSeqNo)))
            else:
                # TODO let's have isValidPrepare throw an exception that gets
                # handled and possibly logged higher
                logger.trace("{} cannot process incoming PREPARE".format(self))
        except SuspiciousNode as ex:
            self.report_suspicious_node(ex)
        return None, None

    def _validate_prepare(self, prepare: Prepare, sender: str) -> bool:
        """
        Return whether the PREPARE specified is valid.

        :param prepare: the PREPARE to validate
        :param sender: the name of the node that sent the PREPARE
        :return: True if PREPARE is valid, False otherwise
        """
        key = (prepare.viewNo, prepare.ppSeqNo)
        primaryStatus = self._is_primary_for_msg(prepare)

        ppReq = self.get_preprepare(*key)

        # If a non primary replica and receiving a PREPARE request before a
        # PRE-PREPARE request, then proceed

        # PREPARE should not be sent from primary
        if self._is_msg_from_primary(prepare, sender):
            self.report_suspicious_node(SuspiciousNode(sender, Suspicions.PR_FRM_PRIMARY, prepare))
            return False

        # If non primary replica
        if primaryStatus is False:
            if self.prepares.hasPrepareFrom(prepare, sender):
                self.report_suspicious_node(SuspiciousNode(
                    sender, Suspicions.DUPLICATE_PR_SENT, prepare))
                return False
            # If PRE-PREPARE not received for the PREPARE, might be slow
            # network
            if not ppReq:
                self._enqueue_prepare(prepare, sender)
                self.l_setup_last_ordered_for_non_master()
                return False
        # If primary replica
        if primaryStatus is True:
            if self.prepares.hasPrepareFrom(prepare, sender):
                self.report_suspicious_node(SuspiciousNode(
                    sender, Suspicions.DUPLICATE_PR_SENT, prepare))
                return False
            # If PRE-PREPARE was not sent for this PREPARE, certainly
            # malicious behavior unless this is re-ordering after view change where a new Primary may not have a
            # PrePrepare from old view
            elif not ppReq:
                if prepare.ppSeqNo <= self._data.prev_view_prepare_cert:
                    self._enqueue_prepare(prepare, sender)
                else:
                    self.report_suspicious_node(SuspiciousNode(
                        sender, Suspicions.UNKNOWN_PR_SENT, prepare))
                return False

        if primaryStatus is None and not ppReq:
            self._enqueue_prepare(prepare, sender)
            self.l_setup_last_ordered_for_non_master()
            return False

        if prepare.digest != ppReq.digest:
            self.report_suspicious_node(SuspiciousNode(sender, Suspicions.PR_DIGEST_WRONG, prepare))
            return False
        elif prepare.stateRootHash != ppReq.stateRootHash:
            self.report_suspicious_node(SuspiciousNode(sender, Suspicions.PR_STATE_WRONG,
                                                       prepare))
            return False
        elif prepare.txnRootHash != ppReq.txnRootHash:
            self.report_suspicious_node(SuspiciousNode(sender, Suspicions.PR_TXN_WRONG,
                                                       prepare))
            return False
        elif prepare.auditTxnRootHash != ppReq.auditTxnRootHash:
            self.report_suspicious_node(SuspiciousNode(sender, Suspicions.PR_AUDIT_TXN_ROOT_HASH_WRONG,
                                                       prepare))
            return False

        # BLS multi-sig:
        self.l_bls_bft_replica.validate_prepare(prepare, sender)

        return True

    """Method from legacy code"""
    def _enqueue_prepare(self, pMsg: Prepare, sender: str):
        key = (pMsg.viewNo, pMsg.ppSeqNo)
        logger.debug("{} queueing prepare due to unavailability of PRE-PREPARE. "
                     "Prepare {} for key {} from {}".format(self, pMsg, key, sender))
        if key not in self.preparesWaitingForPrePrepare:
            self.preparesWaitingForPrePrepare[key] = deque()
        self.preparesWaitingForPrePrepare[key].append((pMsg, sender))
        if key not in self.pre_prepares_stashed_for_incorrect_time:
            if self.is_master or self.last_ordered_3pc[1] != 0:
                self._request_pre_prepare_for_prepare(key)
        else:
            self._process_stashed_pre_prepare_for_time_if_possible(key)

    def _process_stashed_pre_prepare_for_time_if_possible(
            self, key: Tuple[int, int]):
        """
        Check if any PRE-PREPAREs that were stashed since their time was not
        acceptable, can now be accepted since enough PREPAREs are received
        """
        logger.debug('{} going to process stashed PRE-PREPAREs with '
                     'incorrect times'.format(self))
        q = self._data.quorums.f
        if len(self.preparesWaitingForPrePrepare[key]) > q:
            times = [pr.ppTime for (pr, _) in
                     self.preparesWaitingForPrePrepare[key]]
            most_common_time, freq = mostCommonElement(times)
            if self._data.quorums.timestamp.is_reached(freq):
                logger.debug('{} found sufficient PREPAREs for the '
                             'PRE-PREPARE{}'.format(self, key))
                stashed_pp = self.pre_prepares_stashed_for_incorrect_time
                pp, sender, done = stashed_pp[key]
                if done:
                    logger.debug('{} already processed PRE-PREPARE{}'.format(self, key))
                    return True
                # True is set since that will indicate to `is_pre_prepare_time_acceptable`
                # that sufficient PREPAREs are received
                stashed_pp[key] = (pp, sender, True)
                self._network.process_incoming(pp, sender)
                return True
        return False

    def _request_pre_prepare_for_prepare(self, three_pc_key) -> bool:
        """
        Check if has an acceptable PRE_PREPARE already stashed, if not then
        check count of PREPAREs, make sure >f consistent PREPAREs are found,
        store the acceptable PREPARE state (digest, roots) for verification of
        the received PRE-PREPARE
        """

        if three_pc_key in self.prePreparesPendingPrevPP:
            logger.debug('{} not requesting a PRE-PREPARE since already found '
                         'stashed for {}'.format(self, three_pc_key))
            return False

        if len(
                self.preparesWaitingForPrePrepare[three_pc_key]) < self._data.quorums.prepare.value:
            logger.debug(
                '{} not requesting a PRE-PREPARE because does not have'
                ' sufficient PREPAREs for {}'.format(
                    self, three_pc_key))
            return False

        digest, state_root, txn_root, _ = \
            self._get_acceptable_stashed_prepare_state(three_pc_key)

        # Choose a better data structure for `prePreparesPendingFinReqs`
        pre_prepares = [pp for pp, _, _ in self.prePreparesPendingFinReqs
                        if (pp.viewNo, pp.ppSeqNo) == three_pc_key]
        if pre_prepares:
            if [pp for pp in pre_prepares if (pp.digest, pp.stateRootHash, pp.txnRootHash) == (digest, state_root, txn_root)]:
                logger.debug('{} not requesting a PRE-PREPARE since already '
                             'found stashed for {}'.format(self, three_pc_key))
                return False

        self._request_pre_prepare(three_pc_key,
                                  stash_data=(digest, state_root, txn_root))
        return True

    def _get_acceptable_stashed_prepare_state(self, three_pc_key):
        prepares = {s: (m.digest, m.stateRootHash, m.txnRootHash) for m, s in
                    self.preparesWaitingForPrePrepare[three_pc_key]}
        acceptable, freq = mostCommonElement(prepares.values())
        return (*acceptable, {s for s, state in prepares.items()
                              if state == acceptable})

    def _is_primary_for_msg(self, msg) -> Optional[bool]:
        """
        Return whether this replica is primary if the request's view number is
        equal this replica's view number and primary has been selected for
        the current view.
        Return None otherwise.
        :param msg: message
        """
        return self._data.is_primary if self._is_msg_for_current_view(msg) \
            else self._is_primary_in_view(msg.viewNo)

    def _is_primary_in_view(self, viewNo: int) -> Optional[bool]:
        """
        Return whether this replica was primary in the given view
        """
        if viewNo not in self.primary_names:
            return False
        return self.primary_names[viewNo] == self.name

    @measure_consensus_time(MetricsName.PROCESS_COMMIT_TIME,
                            MetricsName.BACKUP_PROCESS_COMMIT_TIME)
    def process_commit(self, commit: Commit, sender: str):
        """
        Validate and process the COMMIT specified.
        If validation is successful, return the message to the node.

        :param commit: an incoming COMMIT message
        :param sender: name of the node that sent the COMMIT
        """
        result, reason = self._validate(commit)
        if result != PROCESS:
            return result, reason

        logger.debug("{} received COMMIT{} from {}".format(self, (commit.viewNo, commit.ppSeqNo), sender))

        if self._validate_commit(commit, sender):
            self.stats.inc(TPCStat.CommitRcvd)
            self._add_to_commits(commit, sender)
            logger.debug("{} processed incoming COMMIT{}".format(
                self, (commit.viewNo, commit.ppSeqNo)))
        return result, reason

    def _validate_commit(self, commit: Commit, sender: str) -> bool:
        """
        Return whether the COMMIT specified is valid.

        :param commit: the COMMIT to validate
        :return: True if `request` is valid, False otherwise
        """
        key = (commit.viewNo, commit.ppSeqNo)
        if not self._has_prepared(key):
            self._enqueue_commit(commit, sender)
            return False

        if self.commits.hasCommitFrom(commit, sender):
            self.report_suspicious_node(SuspiciousNode(sender, Suspicions.DUPLICATE_CM_SENT, commit))
            return False

        # BLS multi-sig (call it for non-ordered only to avoid redundant validations):
        why_not = None
        if not self._validator.has_already_ordered(commit.viewNo, commit.ppSeqNo):
            pre_prepare = self.get_preprepare(commit.viewNo, commit.ppSeqNo)
            why_not = self.l_bls_bft_replica.validate_commit(commit, sender, pre_prepare)

        if why_not == BlsBftReplica.CM_BLS_SIG_WRONG:
            logger.warning("{} discard Commit message from {}:{}".format(self, sender, commit))
            self.report_suspicious_node(SuspiciousNode(sender,
                                                       Suspicions.CM_BLS_SIG_WRONG,
                                                       commit))
            return False
        elif why_not is not None:
            logger.warning("Unknown error code returned for bls commit "
                           "validation {}".format(why_not))

        return True

    def _enqueue_commit(self, request: Commit, sender: str):
        key = (request.viewNo, request.ppSeqNo)
        logger.debug("{} - Queueing commit due to unavailability of PREPARE. "
                     "Request {} with key {} from {}".format(self, request, key, sender))
        if key not in self.commitsWaitingForPrepare:
            self.commitsWaitingForPrepare[key] = deque()
        self.commitsWaitingForPrepare[key].append((request, sender))

    @measure_consensus_time(MetricsName.PROCESS_PREPREPARE_TIME,
                            MetricsName.BACKUP_PROCESS_PREPREPARE_TIME)
    def process_preprepare(self, pre_prepare: PrePrepare, sender: str):
        """
        Validate and process provided PRE-PREPARE, create and
        broadcast PREPARE for it.

        :param pre_prepare: message
        :param sender: name of the node that sent this message
        """
        pp_key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
        # the same PrePrepare might come here multiple times
        if (pp_key and (pre_prepare.auditTxnRootHash, sender) not in self.pre_prepare_tss[pp_key]):
            # TODO more clean solution would be to set timestamps
            # earlier (e.g. in zstack)
            self.pre_prepare_tss[pp_key][pre_prepare.auditTxnRootHash, sender] = self.get_time_for_3pc_batch()

        result, reason = self._validate(pre_prepare)
        if result != PROCESS:
            return result, reason

        key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
        logger.debug("{} received PRE-PREPARE{} from {}".format(self, key, sender))

        # TODO: should we still do it?
        # Converting each req_idrs from list to tuple
        req_idrs = {f.REQ_IDR.nm: [key for key in pre_prepare.reqIdr]}
        pre_prepare = updateNamedTuple(pre_prepare, **req_idrs)

        def report_suspicious(reason):
            ex = SuspiciousNode(sender, reason, pre_prepare)
            self.report_suspicious_node(ex)

        why_not = self._can_process_pre_prepare(pre_prepare, sender)
        if why_not is None:
            why_not_applied = \
                self._process_valid_preprepare(pre_prepare, sender)
            if why_not_applied is not None:
                if why_not_applied == PP_APPLY_REJECT_WRONG:
                    report_suspicious(Suspicions.PPR_REJECT_WRONG)
                elif why_not_applied == PP_APPLY_WRONG_STATE:
                    report_suspicious(Suspicions.PPR_STATE_WRONG)
                elif why_not_applied == PP_APPLY_ROOT_HASH_MISMATCH:
                    report_suspicious(Suspicions.PPR_TXN_WRONG)
                elif why_not_applied == PP_APPLY_HOOK_ERROR:
                    report_suspicious(Suspicions.PPR_PLUGIN_EXCEPTION)
                elif why_not_applied == PP_SUB_SEQ_NO_WRONG:
                    report_suspicious(Suspicions.PPR_SUB_SEQ_NO_WRONG)
                elif why_not_applied == PP_NOT_FINAL:
                    # this is fine, just wait for another
                    return None, None
                elif why_not_applied == PP_APPLY_AUDIT_HASH_MISMATCH:
                    report_suspicious(Suspicions.PPR_AUDIT_TXN_ROOT_HASH_WRONG)
                elif why_not_applied == PP_REQUEST_ALREADY_ORDERED:
                    report_suspicious(Suspicions.PPR_WITH_ORDERED_REQUEST)
                elif why_not_applied == PP_WRONG_PRIMARIES:
                    report_suspicious(Suspicions.PPR_WITH_WRONG_PRIMARIES)

        elif why_not == PP_APPLY_WRONG_DIGEST:
            report_suspicious(Suspicions.PPR_DIGEST_WRONG)
        elif why_not == PP_CHECK_NOT_FROM_PRIMARY:
            report_suspicious(Suspicions.PPR_FRM_NON_PRIMARY)
        elif why_not == PP_CHECK_TO_PRIMARY:
            report_suspicious(Suspicions.PPR_TO_PRIMARY)
        elif why_not == PP_CHECK_DUPLICATE:
            report_suspicious(Suspicions.DUPLICATE_PPR_SENT)
        elif why_not == PP_CHECK_INCORRECT_POOL_STATE_ROOT:
            report_suspicious(Suspicions.PPR_POOL_STATE_ROOT_HASH_WRONG)
        elif why_not == PP_CHECK_OLD:
            logger.info("PRE-PREPARE {} has ppSeqNo lower "
                        "then the latest one - ignoring it".format(key))
        elif why_not == PP_CHECK_REQUEST_NOT_FINALIZED:
            absents = set()
            non_fin = set()
            non_fin_payload = set()
            for key in pre_prepare.reqIdr:
                req = self._requests.get(key)
                if req is None:
                    absents.add(key)
                elif not req.finalised:
                    non_fin.add(key)
                    non_fin_payload.add(req.request.payload_digest)
            absent_str = ', '.join(str(key) for key in absents)
            non_fin_str = ', '.join(
                '{} ({} : {})'.format(str(key),
                                      str(len(self._requests[key].propagates)),
                                      ', '.join(self._requests[key].propagates.keys())) for key in non_fin)
            logger.warning(
                "{} found requests in the incoming pp, of {} ledger, that are not finalized. "
                "{} of them don't have propagates: [{}]. "
                "{} of them don't have enough propagates: [{}].".format(self, pre_prepare.ledgerId,
                                                                        len(absents), absent_str,
                                                                        len(non_fin), non_fin_str))

            def signal_suspicious(req):
                logger.info("Request digest {} already ordered. Discard {} "
                            "from {}".format(req, pre_prepare, sender))
                report_suspicious(Suspicions.PPR_WITH_ORDERED_REQUEST)

            # checking for payload digest is more effective
            for payload_key in non_fin_payload:
                if self.db_manager.get_store(SEQ_NO_DB_LABEL).get_by_payload_digest(payload_key) != (None, None):
                    signal_suspicious(payload_key)
                    return None, None

            # for absents we can only check full digest
            for full_key in absents:
                if self.db_manager.get_store(SEQ_NO_DB_LABEL).get_by_full_digest(full_key) is not None:
                    signal_suspicious(full_key)
                    return None, None

            bad_reqs = absents | non_fin
            self._enqueue_pre_prepare(pre_prepare, sender, bad_reqs)
            # TODO: An optimisation might be to not request PROPAGATEs
            # if some PROPAGATEs are present or a client request is
            # present and sufficient PREPAREs and PRE-PREPARE are present,
            # then the digest can be compared but this is expensive as the
            # PREPARE and PRE-PREPARE contain a combined digest
            if self._config.PROPAGATE_REQUEST_ENABLED:
                self._schedule(partial(self._request_propagates_if_needed, bad_reqs, pre_prepare),
                               self._config.PROPAGATE_REQUEST_DELAY)
        elif why_not == PP_CHECK_NOT_NEXT:
            pp_view_no = pre_prepare.viewNo
            pp_seq_no = pre_prepare.ppSeqNo
            _, last_pp_seq_no = self.__last_pp_3pc
            if self.is_master or self.last_ordered_3pc[1] != 0:
                seq_frm = last_pp_seq_no + 1
                seq_to = pp_seq_no - 1
                if seq_to >= seq_frm >= pp_seq_no - self._config.CHK_FREQ + 1:
                    logger.warning(
                        "{} missing PRE-PREPAREs from {} to {}, "
                        "going to request".format(self, seq_frm, seq_to))
                    self._request_missing_three_phase_messages(
                        pp_view_no, seq_frm, seq_to)
            self._enqueue_pre_prepare(pre_prepare, sender)
            self.l_setup_last_ordered_for_non_master()
        elif why_not == PP_CHECK_WRONG_TIME:
            key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
            item = (pre_prepare, sender, False)
            self.pre_prepares_stashed_for_incorrect_time[key] = item
            report_suspicious(Suspicions.PPR_TIME_WRONG)
        elif why_not == BlsBftReplica.PPR_BLS_MULTISIG_WRONG:
            report_suspicious(Suspicions.PPR_BLS_MULTISIG_WRONG)
        else:
            logger.warning("Unknown PRE-PREPARE check status: {}".format(why_not))
        return None, None

    @property
    def view_no(self):
        return self._data.view_no

    @property
    def sent_preprepares(self):
        return self._data.sent_preprepares

    @property
    def prepares(self):
        return self._data.prepares

    @property
    def commits(self):
        return self._data.commits

    @property
    def requested_pre_prepares(self):
        return self._data.requested_pre_prepares

    @property
    def last_ordered_3pc(self):
        return self._data.last_ordered_3pc

    @last_ordered_3pc.setter
    def last_ordered_3pc(self, lo_tuple):
        self._data.last_ordered_3pc = lo_tuple
        pp_seq_no = lo_tuple[1]
        if pp_seq_no > self.lastPrePrepareSeqNo:
            self.lastPrePrepareSeqNo = pp_seq_no
        logger.info('{} set last ordered as {}'.format(self, lo_tuple))

    @property
    def last_preprepare(self):
        last_3pc = (0, 0)
        lastPp = None
        if self.sent_preprepares:
            (v, s), pp = self.sent_preprepares.peekitem(-1)
            last_3pc = (v, s)
            lastPp = pp
        if self.prePrepares:
            (v, s), pp = self.prePrepares.peekitem(-1)
            if compare_3PC_keys(last_3pc, (v, s)) > 0:
                lastPp = pp
        return lastPp

    @property
    def __last_pp_3pc(self):
        last_pp = self.last_preprepare
        if not last_pp:
            return self.last_ordered_3pc

        last_3pc = (last_pp.viewNo, last_pp.ppSeqNo)
        if compare_3PC_keys(self.last_ordered_3pc, last_3pc) > 0:
            return last_3pc

        return self.last_ordered_3pc

    @property
    def db_manager(self):
        return self._write_manager.database_manager

    @property
    def is_master(self):
        return self._data.is_master

    @property
    def primary_name(self):
        """
        Name of the primary replica of this replica's instance

        :return: Returns name if primary is known, None otherwise
        """
        return self._data.primary_name

    @property
    def name(self):
        return self._data.name

    @name.setter
    def name(self, n):
        self._data._name = n

    @property
    def f(self):
        return getMaxFailures(self._data.total_nodes)

    def gc(self, till3PCKey):
        logger.info("{} cleaning up till {}".format(self, till3PCKey))
        tpcKeys = set()
        reqKeys = set()

        for key3PC, pp in itertools.chain(
            self.sent_preprepares.items(),
            self.prePrepares.items()
        ):
            if compare_3PC_keys(till3PCKey, key3PC) <= 0:
                tpcKeys.add(key3PC)
                for reqKey in pp.reqIdr:
                    reqKeys.add(reqKey)

        for key3PC, pp_dict in self.pre_prepare_tss.items():
            if compare_3PC_keys(till3PCKey, key3PC) <= 0:
                tpcKeys.add(key3PC)
                # TODO INDY-1983: was found that it adds additional
                # requests to clean, need to explore why
                # for (pp, _) in pp_dict:
                #    for reqKey in pp.reqIdr:
                #        reqKeys.add(reqKey)

        logger.trace("{} found {} 3-phase keys to clean".
                     format(self, len(tpcKeys)))
        logger.trace("{} found {} request keys to clean".
                     format(self, len(reqKeys)))

        self.old_view_preprepares = {k: v for k, v in self.old_view_preprepares.items()
                                     if compare_3PC_keys(till3PCKey, (k[0], k[1])) > 0}

        to_clean_up = (
            self.pre_prepare_tss,
            self.sent_preprepares,
            self.prePrepares,
            self.prepares,
            self.commits,
            self.batches,
            self.pre_prepares_stashed_for_incorrect_time,
        )
        for request_key in tpcKeys:
            pp = self.get_preprepare(*request_key)
            if pp:
                self._clear_batch(self.get_preprepare(*request_key))
            for coll in to_clean_up:
                coll.pop(request_key, None)

        for request_key in reqKeys:
            self._requests.ordered_by_replica(request_key)
            self._requests.free(request_key)
            for ledger_id, keys in self.requestQueues.items():
                if request_key in keys:
                    self.discard_req_key(ledger_id, request_key)
            logger.trace('{} freed request {} from previous checkpoints'.format(self, request_key))

        # ToDo: do we need ordered messages there?
        self.ordered.clear_below_view(self.view_no - 1)

        # BLS multi-sig:
        self.l_bls_bft_replica.gc(till3PCKey)

    def discard_req_key(self, ledger_id, req_key):
        self.requestQueues[ledger_id].discard(req_key)

    def _clear_prev_view_pre_prepares(self):
        to_remove = []
        for idx, (pp, _, _) in enumerate(self.prePreparesPendingFinReqs):
            if pp.viewNo < self.view_no:
                to_remove.insert(0, idx)
        for idx in to_remove:
            self.prePreparesPendingFinReqs.pop(idx)

        for (v, p) in list(self.prePreparesPendingPrevPP.keys()):
            if v < self.view_no:
                self.prePreparesPendingPrevPP.pop((v, p))

    def report_suspicious_node(self, ex: SuspiciousNode):
        logger.debug("{} raised suspicion on node {} for {}; suspicion code "
                     "is {}".format(self, ex.node, ex.reason, ex.code))
        self._bus.send(RaisedSuspicion(inst_id=self._data.inst_id,
                                       ex=ex))

    def _validate(self, msg):
        return self._validator.validate(msg)

    def _can_process_pre_prepare(self, pre_prepare: PrePrepare, sender: str):
        """
        Decide whether this replica is eligible to process a PRE-PREPARE.

        :param pre_prepare: a PRE-PREPARE msg to process
        :param sender: the name of the node that sent the PRE-PREPARE msg
        """

        if self._validator.has_already_ordered(pre_prepare.viewNo, pre_prepare.ppSeqNo):
            return None

        digest = self.generate_pp_digest(pre_prepare.reqIdr,
                                         get_original_viewno(pre_prepare),
                                         pre_prepare.ppTime)
        if digest != pre_prepare.digest:
            return PP_APPLY_WRONG_DIGEST

        # PRE-PREPARE should not be sent from non primary
        if not self._is_msg_from_primary(pre_prepare, sender):
            return PP_CHECK_NOT_FROM_PRIMARY

        # Already has a PRE-PREPARE with same 3 phase key
        if (pre_prepare.viewNo, pre_prepare.ppSeqNo) in self.prePrepares:
            return PP_CHECK_DUPLICATE

        if not self._is_pre_prepare_time_acceptable(pre_prepare, sender):
            return PP_CHECK_WRONG_TIME

        if compare_3PC_keys((pre_prepare.viewNo, pre_prepare.ppSeqNo),
                            self.__last_pp_3pc) > 0:
            return PP_CHECK_OLD  # ignore old pre-prepare

        if self._non_finalised_reqs(pre_prepare.reqIdr):
            return PP_CHECK_REQUEST_NOT_FINALIZED

        if not self._is_next_pre_prepare(pre_prepare.viewNo,
                                         pre_prepare.ppSeqNo):
            return PP_CHECK_NOT_NEXT

        if f.POOL_STATE_ROOT_HASH.nm in pre_prepare and \
                pre_prepare.poolStateRootHash != self.get_state_root_hash(POOL_LEDGER_ID):
            return PP_CHECK_INCORRECT_POOL_STATE_ROOT

        # BLS multi-sig:
        status = self.l_bls_bft_replica.validate_pre_prepare(pre_prepare,
                                                             sender)
        if status is not None:
            return status
        return None

    def _schedule(self, func, delay):
        self._timer.schedule(delay, func)

    def _process_valid_preprepare(self, pre_prepare: PrePrepare, sender: str):
        why_not_applied = None
        # apply and validate applied PrePrepare if it's not odered yet
        if not self._validator.has_already_ordered(pre_prepare.viewNo, pre_prepare.ppSeqNo):
            why_not_applied = self._apply_and_validate_applied_pre_prepare(pre_prepare, sender)

        if why_not_applied is not None:
            return why_not_applied

        # add to PrePrepares
        if self._data.is_primary:
            self._add_to_sent_pre_prepares(pre_prepare)
        else:
            self._add_to_pre_prepares(pre_prepare)

        # we may have stashed Prepares and Commits if this is PrePrepare from old view (re-ordering phase)
        self._dequeue_prepares(pre_prepare.viewNo, pre_prepare.ppSeqNo)
        self._dequeue_commits(pre_prepare.viewNo, pre_prepare.ppSeqNo)

        return None

    def _apply_and_validate_applied_pre_prepare(self, pre_prepare: PrePrepare, sender: str):
        self.first_batch_after_catchup = False
        old_state_root = self.get_state_root_hash(pre_prepare.ledgerId, to_str=False)
        old_txn_root = self.get_txn_root_hash(pre_prepare.ledgerId)
        if self.is_master:
            logger.debug('{} state root before processing {} is {}, {}'.format(
                self,
                pre_prepare,
                old_state_root,
                old_txn_root))

        # 1. APPLY
        reqs, invalid_indices, rejects, suspicious = self._apply_pre_prepare(pre_prepare)

        # 2. CHECK IF MORE CHUNKS NEED TO BE APPLIED FURTHER BEFORE VALIDATION
        if pre_prepare.sub_seq_no != 0:
            return PP_SUB_SEQ_NO_WRONG

        if not pre_prepare.final:
            return PP_NOT_FINAL

        # 3. VALIDATE APPLIED
        invalid_from_pp = invalid_index_serializer.deserialize(pre_prepare.discarded)
        if suspicious:
            why_not_applied = PP_REQUEST_ALREADY_ORDERED
        else:
            why_not_applied = self._validate_applied_pre_prepare(pre_prepare,
                                                                 invalid_indices, invalid_from_pp)

        # 4. IF NOT VALID AFTER APPLYING - REVERT
        if why_not_applied is not None:
            if self.is_master:
                self._revert(pre_prepare.ledgerId,
                             old_state_root,
                             len(pre_prepare.reqIdr) - len(invalid_indices))
            return why_not_applied

        # 5. TRACK APPLIED
        if rejects:
            for reject in rejects:
                self._network.send(reject)

        if self.is_master:
            # BLS multi-sig:
            self.l_bls_bft_replica.process_pre_prepare(pre_prepare, sender)
            logger.trace("{} saved shared multi signature for root".format(self, old_state_root))

        if not self.is_master:
            self.db_manager.get_store(LAST_SENT_PP_STORE_LABEL).store_last_sent_pp_seq_no(
                self._data.inst_id, pre_prepare.ppSeqNo)
        self._track_batches(pre_prepare, old_state_root)
        key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
        logger.debug("{} processed incoming PRE-PREPARE{}".format(self, key),
                     extra={"tags": ["processing"]})
        return None

    def _enqueue_pre_prepare(self, pre_prepare: PrePrepare, sender: str,
                             nonFinReqs: Set = None):
        if nonFinReqs:
            logger.info("{} - Queueing pre-prepares due to unavailability of finalised "
                        "requests. PrePrepare {} from {}".format(self, pre_prepare, sender))
            self.prePreparesPendingFinReqs.append((pre_prepare, sender, nonFinReqs))
        else:
            # Possible exploit, an malicious party can send an invalid
            # pre-prepare and over-write the correct one?
            logger.info("Queueing pre-prepares due to unavailability of previous pre-prepares. {} from {}".
                        format(pre_prepare, sender))
            self.prePreparesPendingPrevPP[pre_prepare.viewNo, pre_prepare.ppSeqNo] = (pre_prepare, sender)

    def _request_propagates_if_needed(self, bad_reqs: list, pre_prepare: PrePrepare):
        if any(pre_prepare is pended[0] for pended in self.prePreparesPendingFinReqs):
            self._bus.send(RequestPropagates(bad_reqs))

    def _request_missing_three_phase_messages(self, view_no: int, seq_frm: int, seq_to: int) -> None:
        for pp_seq_no in range(seq_frm, seq_to + 1):
            key = (view_no, pp_seq_no)
            self._request_pre_prepare(key)
            self._request_prepare(key)
            self._request_commit(key)

    def _request_three_phase_msg(self, three_pc_key: Tuple[int, int],
                                 msg_type: str,
                                 recipients: Optional[List[str]] = None,
                                 stash_data: Optional[Tuple[str, str, str]] = None):
        self._bus.send(MissingMessage(msg_type,
                                      three_pc_key,
                                      self._data.inst_id,
                                      recipients,
                                      stash_data))

    def _request_pre_prepare(self, three_pc_key: Tuple[int, int],
                             stash_data: Optional[Tuple[str, str, str]] = None):
        """
        Request preprepare
        """
        if not self._config.PRE_PREPARE_REQUEST_ENABLED:
            return
        recipients = [replica_name_to_node_name(self.primary_name)]
        self._request_three_phase_msg(three_pc_key,
                                      PREPREPARE,
                                      recipients,
                                      stash_data)

    def _request_prepare(self, three_pc_key: Tuple[int, int],
                         recipients: List[str] = None,
                         stash_data: Optional[Tuple[str, str, str]] = None):
        """
        Request preprepare
        """
        if not self._config.PREPARE_REQUEST_ENABLED:
            return
        if recipients is None:
            recipients = self._network.connecteds.copy()
            primary_node_name = replica_name_to_node_name(self.primary_name)
            if primary_node_name in recipients:
                recipients.remove(primary_node_name)
        return self._request_three_phase_msg(three_pc_key, PREPARE, recipients, stash_data)

    def _request_commit(self, three_pc_key: Tuple[int, int],
                        recipients: List[str] = None):
        """
        Request commit
        """
        if not self._config.COMMIT_REQUEST_ENABLED:
            return
        if recipients is None:
            recipients = self._network.connecteds.copy()
        self._request_three_phase_msg(three_pc_key, COMMIT, recipients)

    """Method from legacy code"""
    def l_setup_last_ordered_for_non_master(self):
        """
        Since last ordered view_no and pp_seq_no are only communicated for
        master instance, backup instances use this method for restoring
        `last_ordered_3pc`
        :return:
        """
        if not self.is_master and self.first_batch_after_catchup and \
                not self._data.is_primary:
            # If not master instance choose last ordered seq no to be 1 less
            # the lowest prepared certificate in this view
            lowest_prepared = self.l_get_lowest_probable_prepared_certificate_in_view(
                self.view_no)
            if lowest_prepared is not None:
                # now after catch up we have in last_ordered_3pc[1] value 0
                # it value should change last_ordered_3pc to lowest_prepared - 1
                logger.info('{} Setting last ordered for non-master as {}'.
                            format(self, self.last_ordered_3pc))
                self.last_ordered_3pc = (self.view_no, lowest_prepared - 1)
                self._bus.send(BackupSetupLastOrdered(inst_id=self._data.inst_id))
                self.first_batch_after_catchup = False

    def get_state_root_hash(self, ledger_id: str, to_str=True, committed=False):
        return self.db_manager.get_state_root_hash(ledger_id, to_str, committed) \
            if self.is_master \
            else None

    def get_txn_root_hash(self, ledger_id: str, to_str=True):
        return self.db_manager.get_txn_root_hash(ledger_id, to_str) \
            if self.is_master \
            else None

    def _is_msg_from_primary(self, msg, sender: str) -> bool:
        """
        Return whether this message was from primary replica
        :param msg:
        :param sender:
        :return:
        """
        if self._is_msg_for_current_view(msg):
            return self.primary_name == sender
        try:
            return self.primary_names[msg.viewNo] == sender
        except KeyError:
            return False

    def _is_msg_for_current_view(self, msg):
        """
        Return whether this request's view number is equal to the current view
        number of this replica.
        """
        viewNo = getattr(msg, "viewNo", None)
        return viewNo == self.view_no

    def _is_pre_prepare_time_correct(self, pp: PrePrepare, sender: str) -> bool:
        """
        Check if this PRE-PREPARE is not older than (not checking for greater
        than since batches maybe sent in less than 1 second) last PRE-PREPARE
        and in a sufficient range of local clock's UTC time.
        :param pp:
        :return:
        """
        tpcKey = (pp.viewNo, pp.ppSeqNo)

        if (self.last_accepted_pre_prepare_time and
                pp.ppTime < self.last_accepted_pre_prepare_time):
            return False
        elif ((tpcKey not in self.pre_prepare_tss) or
                ((pp.auditTxnRootHash, sender) not in self.pre_prepare_tss[tpcKey])):
            return False
        else:
            return (
                abs(pp.ppTime - self.pre_prepare_tss[tpcKey][pp.auditTxnRootHash, sender]) <=
                self._config.ACCEPTABLE_DEVIATION_PREPREPARE_SECS
            )

    def _is_pre_prepare_time_acceptable(self, pp: PrePrepare, sender: str) -> bool:
        """
        Returns True or False depending on the whether the time in PRE-PREPARE
        is acceptable. Can return True if time is not acceptable but sufficient
        PREPAREs are found to support the PRE-PREPARE
        :param pp:
        :return:
        """
        key = (pp.viewNo, pp.ppSeqNo)
        if key in self.requested_pre_prepares:
            # Special case for requested PrePrepares
            return True
        correct = self._is_pre_prepare_time_correct(pp, sender)
        if not correct:
            if key in self.pre_prepares_stashed_for_incorrect_time and \
                    self.pre_prepares_stashed_for_incorrect_time[key][-1]:
                logger.debug('{} marking time as correct for {}'.format(self, pp))
                correct = True
            else:
                logger.warning('{} found {} to have incorrect time.'.format(self, pp))
        return correct

    def _non_finalised_reqs(self, reqKeys: List[Tuple[str, int]]):
        """
        Check if there are any requests which are not finalised, i.e for
        which there are not enough PROPAGATEs
        """
        # TODO: fix comment, write tests
        return {key for key in reqKeys if key not in self._requests}
        # return {key for key in reqKeys if not self._requests.is_finalised(key)}

    """Method from legacy code"""
    def _is_next_pre_prepare(self, view_no: int, pp_seq_no: int):
        if pp_seq_no == 1:
            # First PRE-PREPARE
            return True
        (last_pp_view_no, last_pp_seq_no) = self.__last_pp_3pc

        return pp_seq_no - last_pp_seq_no == 1

    def _apply_pre_prepare(self, pre_prepare: PrePrepare):
        """
        Applies (but not commits) requests of the PrePrepare
        to the ledger and state
        """
        reqs = []
        idx = 0
        rejects = []
        invalid_indices = []
        suspicious = False

        # 1. apply each request
        for req_key in pre_prepare.reqIdr:
            req = self._requests[req_key].request
            try:
                self._process_req_during_batch(req,
                                               pre_prepare.ppTime)
            except (InvalidClientMessageException, UnknownIdentifier, SuspiciousPrePrepare) as ex:
                logger.warning('{} encountered exception {} while processing {}, '
                               'will reject'.format(self, ex, req))
                rejects.append((req.key, Reject(req.identifier, req.reqId, ex)))
                invalid_indices.append(idx)
                if isinstance(ex, SuspiciousPrePrepare):
                    suspicious = True
            finally:
                reqs.append(req)
            idx += 1

        # 2. call callback for the applied batch
        if self.is_master:
            three_pc_batch = ThreePcBatch.from_pre_prepare(
                pre_prepare,
                state_root=self.get_state_root_hash(pre_prepare.ledgerId, to_str=False),
                txn_root=self.get_txn_root_hash(pre_prepare.ledgerId, to_str=False),
                valid_digests=self._get_valid_req_ids_from_all_requests(reqs, invalid_indices)
            )
            self.post_batch_creation(three_pc_batch)

        return reqs, invalid_indices, rejects, suspicious

    def _get_valid_req_ids_from_all_requests(self, reqs, invalid_indices):
        return [req.key for idx, req in enumerate(reqs) if idx not in invalid_indices]

    def _validate_applied_pre_prepare(self, pre_prepare: PrePrepare,
                                      invalid_indices, invalid_from_pp) -> Optional[int]:
        if len(invalid_indices) != len(invalid_from_pp):
            return PP_APPLY_REJECT_WRONG

        if self.is_master:
            if pre_prepare.stateRootHash != self.get_state_root_hash(pre_prepare.ledgerId):
                return PP_APPLY_WRONG_STATE

            if pre_prepare.txnRootHash != self.get_txn_root_hash(pre_prepare.ledgerId):
                return PP_APPLY_ROOT_HASH_MISMATCH

            # TODO: move this kind of validation to batch handlers
            if f.AUDIT_TXN_ROOT_HASH.nm in pre_prepare and pre_prepare.auditTxnRootHash != self.get_txn_root_hash(AUDIT_LEDGER_ID):
                return PP_APPLY_AUDIT_HASH_MISMATCH

        return None

    """Method from legacy code"""
    def l_get_lowest_probable_prepared_certificate_in_view(
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

        for (v, p) in self.prePreparesPendingPrevPP:
            if v == view_no:
                seq_no_pp.add(p)
            if v > view_no:
                break

        for (v, p), pr in self.preparesWaitingForPrePrepare.items():
            if v == view_no and len(pr) >= self._data.quorums.prepare.value:
                seq_no_p.add(p)

        for n in seq_no_pp:
            if n in seq_no_p:
                return n
        return None

    def _revert(self, ledgerId, stateRootHash, reqCount):
        # A batch should only be reverted if all batches that came after it
        # have been reverted
        ledger = self.db_manager.get_ledger(ledgerId)
        state = self.db_manager.get_state(ledgerId)
        logger.info('{} reverting {} txns and state root from {} to {} for ledger {}'
                    .format(self, reqCount, Ledger.hashToStr(state.headHash),
                            Ledger.hashToStr(stateRootHash), ledgerId))
        state.revertToHead(stateRootHash)
        ledger.discardTxns(reqCount)
        self.post_batch_rejection(ledgerId)

    def _track_batches(self, pp: PrePrepare, prevStateRootHash):
        # pp.discarded indicates the index from where the discarded requests
        #  starts hence the count of accepted requests, prevStateRoot is
        # tracked to revert this PRE-PREPARE
        logger.trace('{} tracking batch for {} with state root {}'.format(self, pp, prevStateRootHash))
        if self.is_master:
            self.metrics.add_event(MetricsName.THREE_PC_BATCH_SIZE, len(pp.reqIdr))
        else:
            self.metrics.add_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, len(pp.reqIdr))

        self.batches[(pp.viewNo, pp.ppSeqNo)] = [pp.ledgerId, pp.discarded,
                                                 pp.ppTime, prevStateRootHash, len(pp.reqIdr)]

    @property
    def lastPrePrepareSeqNo(self):
        return self._lastPrePrepareSeqNo

    @lastPrePrepareSeqNo.setter
    def lastPrePrepareSeqNo(self, n):
        """
        This will _lastPrePrepareSeqNo to values greater than its previous
        values else it will not. To forcefully override as in case of `revert`,
        directly set `self._lastPrePrepareSeqNo`
        """
        if n > self._lastPrePrepareSeqNo:
            self._lastPrePrepareSeqNo = n
        else:
            logger.debug(
                '{} cannot set lastPrePrepareSeqNo to {} as its '
                'already {}'.format(
                    self, n, self._lastPrePrepareSeqNo))

    def _add_to_pre_prepares(self, pp: PrePrepare) -> None:
        """
        Add the specified PRE-PREPARE to this replica's list of received
        PRE-PREPAREs and try sending PREPARE

        :param pp: the PRE-PREPARE to add to the list
        """
        key = (pp.viewNo, pp.ppSeqNo)
        # ToDo:
        self.prePrepares[key] = pp
        self._preprepare_batch(pp)
        self.lastPrePrepareSeqNo = pp.ppSeqNo
        self.last_accepted_pre_prepare_time = pp.ppTime
        self.stats.inc(TPCStat.PrePrepareRcvd)
        self.try_prepare(pp)

    def _add_to_sent_pre_prepares(self, pp: PrePrepare) -> None:
        self.sent_preprepares[pp.viewNo, pp.ppSeqNo] = pp
        self._preprepare_batch(pp)
        self.lastPrePrepareSeqNo = pp.ppSeqNo

    def _dequeue_prepares(self, viewNo: int, ppSeqNo: int):
        key = (viewNo, ppSeqNo)
        if key in self.preparesWaitingForPrePrepare:
            i = 0
            # Keys of pending prepares that will be processed below
            while self.preparesWaitingForPrePrepare[key]:
                prepare, sender = self.preparesWaitingForPrePrepare[
                    key].popleft()
                logger.debug("{} popping stashed PREPARE{}".format(self, key))
                self._network.process_incoming(prepare, sender)
                i += 1
            self.preparesWaitingForPrePrepare.pop(key)
            logger.debug("{} processed {} PREPAREs waiting for PRE-PREPARE for"
                         " view no {} and seq no {}".format(self, i, viewNo, ppSeqNo))

    def _dequeue_commits(self, viewNo: int, ppSeqNo: int):
        key = (viewNo, ppSeqNo)
        if key in self.commitsWaitingForPrepare:
            if not self._has_prepared(key):
                logger.debug('{} has not pre-prepared {}, will dequeue the '
                             'COMMITs later'.format(self, key))
                return
            i = 0
            # Keys of pending prepares that will be processed below
            while self.commitsWaitingForPrepare[key]:
                commit, sender = self.commitsWaitingForPrepare[
                    key].popleft()
                logger.debug("{} popping stashed COMMIT{}".format(self, key))
                self._network.process_incoming(commit, sender)

                i += 1
            self.commitsWaitingForPrepare.pop(key)
            logger.debug("{} processed {} COMMITs waiting for PREPARE for"
                         " view no {} and seq no {}".format(self, i, viewNo, ppSeqNo))

    def try_prepare(self, pp: PrePrepare):
        """
        Try to send the Prepare message if the PrePrepare message is ready to
        be passed into the Prepare phase.
        """
        rv, msg = self._can_prepare(pp)
        if rv:
            self._do_prepare(pp)
        else:
            logger.debug("{} cannot send PREPARE since {}".format(self, msg))

    def _can_prepare(self, ppReq) -> (bool, str):
        """
        Return whether the batch of requests in the PRE-PREPARE can
        proceed to the PREPARE step.

        :param ppReq: any object with identifier and requestId attributes
        """
        if self.prepares.hasPrepareFrom(ppReq, self.name):
            return False, 'has already sent PREPARE for {}'.format(ppReq)
        return True, ''

    @measure_consensus_time(MetricsName.SEND_PREPARE_TIME,
                            MetricsName.BACKUP_SEND_PREPARE_TIME)
    def _do_prepare(self, pp: PrePrepare):
        logger.debug("{} Sending PREPARE{} at {}".format(self, (pp.viewNo, pp.ppSeqNo), self.get_current_time()))
        params = [self._data.inst_id,
                  pp.viewNo,
                  pp.ppSeqNo,
                  pp.ppTime,
                  pp.digest,
                  pp.stateRootHash,
                  pp.txnRootHash]
        if f.AUDIT_TXN_ROOT_HASH.nm in pp:
            params.append(pp.auditTxnRootHash)

        # BLS multi-sig:
        params = self.l_bls_bft_replica.update_prepare(params, pp.ledgerId)

        prepare = Prepare(*params)
        self._send(prepare, stat=TPCStat.PrepareSent)
        self._add_to_prepares(prepare, self.name)

    def _has_prepared(self, key):
        if not self.get_preprepare(*key):
            return False
        if ((key not in self.prepares and key not in self.sent_preprepares) and
                (key not in self.preparesWaitingForPrePrepare)):
            return False
        return True

    def get_preprepare(self, viewNo, ppSeqNo):
        key = (viewNo, ppSeqNo)
        if key in self.sent_preprepares:
            return self.sent_preprepares[key]
        if key in self.prePrepares:
            return self.prePrepares[key]
        return None

    def _add_to_prepares(self, prepare: Prepare, sender: str):
        """
        Add the specified PREPARE to this replica's list of received
        PREPAREs and try sending COMMIT

        :param prepare: the PREPARE to add to the list
        """
        # BLS multi-sig:
        self.l_bls_bft_replica.process_prepare(prepare, sender)

        self.prepares.addVote(prepare, sender)
        self._dequeue_commits(prepare.viewNo, prepare.ppSeqNo)
        self._try_commit(prepare)

    def _try_commit(self, prepare: Prepare):
        """
        Try to commit if the Prepare message is ready to be passed into the
        commit phase.
        """
        rv, reason = self._can_commit(prepare)
        if rv:
            pp = self.get_preprepare(prepare.viewNo, prepare.ppSeqNo)
            self._prepare_batch(pp)
            self._do_commit(prepare)
        else:
            logger.debug("{} cannot send COMMIT since {}".format(self, reason))

    @measure_consensus_time(MetricsName.SEND_COMMIT_TIME,
                            MetricsName.BACKUP_SEND_COMMIT_TIME)
    def _do_commit(self, p: Prepare):
        """
        Create a commit message from the given Prepare message and trigger the
        commit phase
        :param p: the prepare message
        """
        key_3pc = (p.viewNo, p.ppSeqNo)
        logger.debug("{} Sending COMMIT{} at {}".format(self, key_3pc, self.get_current_time()))

        params = [
            self._data.inst_id, p.viewNo, p.ppSeqNo
        ]

        # BLS multi-sig:
        if p.stateRootHash is not None:
            pre_prepare = self.get_preprepare(*key_3pc)
            params = self.l_bls_bft_replica.update_commit(params, pre_prepare)

        commit = Commit(*params)

        self._send(commit, stat=TPCStat.CommitSent)
        self._add_to_commits(commit, self.name)

    def _add_to_commits(self, commit: Commit, sender: str):
        """
        Add the specified COMMIT to this replica's list of received
        commit requests.

        :param commit: the COMMIT to add to the list
        :param sender: the name of the node that sent the COMMIT
        """
        # BLS multi-sig:
        self.l_bls_bft_replica.process_commit(commit, sender)

        self.commits.addVote(commit, sender)
        self._try_order(commit)

    def _try_order(self, commit: Commit):
        """
        Try to order if the Commit message is ready to be ordered.
        """
        if self._validator.has_already_ordered(commit.viewNo, commit.ppSeqNo):
            self._try_finish_reordering_after_vc(commit.ppSeqNo)

        canOrder, reason = self._can_order(commit)
        if canOrder:
            logger.trace("{} returning request to node".format(self))
            self._do_order(commit)
        else:
            logger.trace("{} cannot return request to node: {}".format(self, reason))

        return canOrder

    def _try_finish_reordering_after_vc(self, pp_seq_no):
        if self.is_master and self._data.prev_view_prepare_cert + 1 == pp_seq_no:
            self._bus.send(MasterReorderedAfterVC())
            self._stasher.process_all_stashed(STASH_WAITING_FIRST_BATCH_IN_VIEW)

    def _do_order(self, commit: Commit):
        key = (commit.viewNo, commit.ppSeqNo)
        logger.debug("{} ordering COMMIT {}".format(self, key))
        return self._order_3pc_key(key)

    @measure_consensus_time(MetricsName.ORDER_3PC_BATCH_TIME,
                            MetricsName.BACKUP_ORDER_3PC_BATCH_TIME)
    def _order_3pc_key(self, key):
        pp = self.get_preprepare(*key)
        if pp is None:
            raise ValueError(
                "{} no PrePrepare with a 'key' {} found".format(self, key)
            )

        self._freshness_checker.update_freshness(ledger_id=pp.ledgerId,
                                                 ts=pp.ppTime)
        self._data.last_batch_timestamp = pp.ppTime

        self._add_to_ordered(*key)
        invalid_indices = invalid_index_serializer.deserialize(pp.discarded)
        invalid_reqIdr = []
        valid_reqIdr = []
        for ind, reqIdr in enumerate(pp.reqIdr):
            if ind in invalid_indices:
                invalid_reqIdr.append(reqIdr)
            else:
                valid_reqIdr.append(reqIdr)
            self._requests.ordered_by_replica(reqIdr)

        original_view_no = get_original_viewno(pp)
        # TODO: Replace Ordered msg by ThreePcBatch
        ordered = Ordered(
            self._data.inst_id,
            pp.viewNo,
            valid_reqIdr,
            invalid_reqIdr,
            pp.ppSeqNo,
            pp.ppTime,
            pp.ledgerId,
            pp.stateRootHash,
            pp.txnRootHash,
            pp.auditTxnRootHash if f.AUDIT_TXN_ROOT_HASH.nm in pp else None,
            self._get_primaries_for_ordered(pp),
            self._get_node_reg_for_ordered(pp),
            original_view_no,
            pp.digest,
        )
        self._discard_ordered_req_keys(pp)

        # BLS multi-sig:
        self.l_bls_bft_replica.process_order(key, self._data.quorums, pp)

        self._bus.send(ordered)

        ordered_msg = "{} ordered batch request, view no {}, ppSeqNo {}, ledger {}, " \
                      "state root {}, txn root {}, audit root {}".format(self, pp.viewNo, pp.ppSeqNo, pp.ledgerId,
                                                                         pp.stateRootHash, pp.txnRootHash,
                                                                         pp.auditTxnRootHash)
        logger.debug("{}, requests ordered {}, discarded {}".
                     format(ordered_msg, valid_reqIdr, invalid_reqIdr))
        logger.info("{}, requests ordered {}, discarded {}".
                    format(ordered_msg, len(valid_reqIdr), len(invalid_reqIdr)))

        if self.is_master:
            self.metrics.add_event(MetricsName.ORDERED_BATCH_SIZE, len(valid_reqIdr) + len(invalid_reqIdr))
            self.metrics.add_event(MetricsName.ORDERED_BATCH_INVALID_COUNT, len(invalid_reqIdr))
        else:
            self.metrics.add_event(MetricsName.BACKUP_ORDERED_BATCH_SIZE, len(valid_reqIdr))

        # do it after Ordered msg is sent
        self._try_finish_reordering_after_vc(key[1])

        return True

    def _add_to_ordered(self, view_no: int, pp_seq_no: int):
        self.ordered.add(view_no, pp_seq_no)
        self.last_ordered_3pc = (view_no, pp_seq_no)

    def _get_primaries_for_ordered(self, pp):
        txn_primaries = self._get_from_audit_for_ordered(pp, AUDIT_TXN_PRIMARIES)
        if txn_primaries is None:
            # TODO: it's possible to get into this case if we have txns being ordered after catch-up is finished
            # when we have no batches applied (uncommitted txns are  reverted when catchup is started)
            # Re-applying of batches will be done in apply_stashed_reqs in node.py,
            # but as we need to fill primaries field in Ordered, we have to emulate what NodeRegHandler would do here
            # TODO: fix this by getting rid of Ordered msg and using ThreePcBatch instead
            txn_primaries = self._write_manager.primary_reg_handler.primaries_selector.select_primaries(self.view_no)
        return txn_primaries

    def _get_node_reg_for_ordered(self, pp):
        txn_node_reg = self._get_from_audit_for_ordered(pp, AUDIT_TXN_NODE_REG)
        if txn_node_reg is None:
            # TODO: it's possible to get into this case if we have txns being ordered after catch-up is finished
            # when we have no batches applied (uncommitted txns are  reverted when catchup is started)
            # Re-applying of batches will be done in apply_stashed_reqs in node.py,
            # but as we need to fill node_reg field in Ordered, we have to emulate what NodeRegHandler would do here
            # TODO: fix this by getting rid of Ordered msg and using ThreePcBatch instead
            txn_node_reg = list(self._write_manager.node_reg_handler.uncommitted_node_reg)
        return txn_node_reg

    def _get_from_audit_for_ordered(self, pp, field):
        if not self.is_master:
            return []
        ledger = self.db_manager.get_ledger(AUDIT_LEDGER_ID)
        for index, txn in enumerate(ledger.get_uncommitted_txns()):
            payload_data = get_payload_data(txn)
            pp_view_no = get_original_viewno(pp)
            if pp.ppSeqNo == payload_data[AUDIT_TXN_PP_SEQ_NO] and \
                    pp_view_no == payload_data[AUDIT_TXN_VIEW_NO]:
                txn_data = payload_data.get(field)
                if isinstance(txn_data, Iterable):
                    return txn_data
                elif isinstance(txn_data, int):
                    last_txn_seq_no = get_seq_no(txn) - txn_data
                    return get_payload_data(
                        ledger.get_by_seq_no_uncommitted(last_txn_seq_no)).get(field)
                break
        return None

    def _discard_ordered_req_keys(self, pp: PrePrepare):
        for k in pp.reqIdr:
            # Using discard since the key may not be present as in case of
            # primary, the key was popped out while creating PRE-PREPARE.
            # Or in case of node catching up, it will not validate
            # PRE-PREPAREs or PREPAREs but will only validate number of COMMITs
            #  and their consistency with PRE-PREPARE of PREPAREs
            self.discard_req_key(pp.ledgerId, k)

    def _can_order(self, commit: Commit) -> Tuple[bool, Optional[str]]:
        """
        Return whether the specified commitRequest can be returned to the node.

        Decision criteria:

        - If have got just n-f Commit requests then return request to node
        - If less than n-f of commit requests then probably don't have
            consensus on the request; don't return request to node
        - If more than n-f then already returned to node; don't return request
            to node

        :param commit: the COMMIT
        """
        quorum = self._data.quorums.commit.value
        if not self.commits.hasQuorum(commit, quorum):
            return False, "no quorum ({}): {} commits where f is {}". \
                format(quorum, commit, self.f)

        key = (commit.viewNo, commit.ppSeqNo)
        if self._validator.has_already_ordered(*key):
            return False, "already ordered"

        if commit.ppSeqNo > 1 and not self._all_prev_ordered(commit):
            viewNo, ppSeqNo = commit.viewNo, commit.ppSeqNo
            if viewNo not in self.stashed_out_of_order_commits:
                self.stashed_out_of_order_commits[viewNo] = {}
            self.stashed_out_of_order_commits[viewNo][ppSeqNo] = commit
            self._out_of_order_repeater.start()
            return False, "stashing {} since out of order". \
                format(commit)

        return True, None

    def _process_stashed_out_of_order_commits(self):
        # This method is called periodically to check for any commits that
        # were stashed due to lack of commits before them and orders them if it
        # can

        if not self.can_order_commits():
            return

        logger.debug('{} trying to order from out of order commits. '
                     'Len(stashed_out_of_order_commits) == {}'
                     .format(self, len(self.stashed_out_of_order_commits)))
        if self.last_ordered_3pc:
            lastOrdered = self.last_ordered_3pc
            vToRemove = set()
            for v in self.stashed_out_of_order_commits:
                if v < lastOrdered[0]:
                    logger.debug(
                        "{} found commits {} from previous view {}"
                        " that were not ordered but last ordered"
                        " is {}".format(
                            self, self.stashed_out_of_order_commits[v], v, lastOrdered))
                    vToRemove.add(v)
                    continue
                pToRemove = set()
                for p, commit in self.stashed_out_of_order_commits[v].items():
                    if (v, p) in self.ordered or \
                            self._validator.has_already_ordered(*(commit.viewNo, commit.ppSeqNo)):
                        pToRemove.add(p)
                        continue
                    if (v == lastOrdered[0] and lastOrdered == (v, p - 1)) or \
                            (v > lastOrdered[0] and self._is_lowest_commit_in_view(commit)):
                        logger.debug("{} ordering stashed commit {}".format(self, commit))
                        if self._try_order(commit):
                            lastOrdered = (v, p)
                            pToRemove.add(p)

                for p in pToRemove:
                    del self.stashed_out_of_order_commits[v][p]
                if not self.stashed_out_of_order_commits[v]:
                    vToRemove.add(v)

            for v in vToRemove:
                del self.stashed_out_of_order_commits[v]

            if not self.stashed_out_of_order_commits:
                self._out_of_order_repeater.stop()
        else:
            logger.debug('{} last_ordered_3pc if False. '
                         'Len(stashed_out_of_order_commits) == {}'
                         .format(self, len(self.stashed_out_of_order_commits)))

    def _is_lowest_commit_in_view(self, commit):
        view_no = commit.viewNo
        if view_no > self.view_no:
            logger.debug('{} encountered {} which belongs to a later view'.format(self, commit))
            return False
        return commit.ppSeqNo == 1

    def _all_prev_ordered(self, commit: Commit):
        """
        Return True if all previous COMMITs have been ordered
        """
        # TODO: This method does a lot of work, choose correct data
        # structures to make it efficient.

        viewNo, ppSeqNo = commit.viewNo, commit.ppSeqNo

        if self.last_ordered_3pc[1] == ppSeqNo - 1:
            # Last ordered was in same view as this COMMIT
            return True

        # if some PREPAREs/COMMITs were completely missed in the same view
        toCheck = set()
        toCheck.update(set(self.sent_preprepares.keys()))
        toCheck.update(set(self.prePrepares.keys()))
        toCheck.update(set(self.prepares.keys()))
        toCheck.update(set(self.commits.keys()))
        for (v, p) in toCheck:
            if v < viewNo and (v, p) not in self.ordered:
                # Have commits from previous view that are unordered.
                return False
            if v == viewNo and p < ppSeqNo and (v, p) not in self.ordered:
                # If unordered commits are found with lower ppSeqNo then this
                # cannot be ordered.
                return False

        return True

    def _can_commit(self, prepare: Prepare) -> (bool, str):
        """
        Return whether the specified PREPARE can proceed to the Commit
        step.

        Decision criteria:

        - If this replica has got just n-f-1 PREPARE requests then commit request.
        - If less than n-f-1 PREPARE requests then probably there's no consensus on
            the request; don't commit
        - If more than n-f-1 then already sent COMMIT; don't commit

        :param prepare: the PREPARE
        """
        quorum = self._data.quorums.prepare.value
        if not self.prepares.hasQuorum(prepare, quorum):
            return False, 'does not have prepare quorum for {}'.format(prepare)
        if self._has_committed(prepare):
            return False, 'has already sent COMMIT for {}'.format(prepare)
        return True, ''

    def _has_committed(self, request) -> bool:
        return self.commits.hasCommitFrom(ThreePhaseKey(
            request.viewNo, request.ppSeqNo), self.name)

    def _is_the_last_old_preprepare(self, pp_seq_no):
        return self._data.prev_view_prepare_cert == pp_seq_no

    def post_batch_creation(self, three_pc_batch: ThreePcBatch):
        """
        A batch of requests has been created and has been applied but
        committed to ledger and state.
        :param ledger_id:
        :param state_root: state root after the batch creation
        :return:
        """
        ledger_id = three_pc_batch.ledger_id
        if self._write_manager.is_valid_ledger_id(ledger_id):
            self._write_manager.post_apply_batch(three_pc_batch)
        else:
            logger.debug('{} did not know how to handle for ledger {}'.format(self, ledger_id))

    def post_batch_rejection(self, ledger_id):
        """
        A batch of requests has been rejected, if stateRoot is None, reject
        the current batch.
        :param ledger_id:
        :param stateRoot: state root after the batch was created
        :return:
        """
        if self._write_manager.is_valid_ledger_id(ledger_id):
            self._write_manager.post_batch_rejected(ledger_id)
        else:
            logger.debug('{} did not know how to handle for ledger {}'.format(self, ledger_id))

    def _ledger_id_for_request(self, request: Request):
        if request.operation.get(TXN_TYPE) is None:
            raise ValueError(
                "{} TXN_TYPE is not defined for request {}".format(self, request)
            )

        typ = request.operation[TXN_TYPE]
        return self._write_manager.type_to_ledger_id[typ]

    def _do_dynamic_validation(self, request: Request, req_pp_time: int):
        """
        State based validation
        """
        # Digest validation
        # TODO implicit caller's context: request is processed by (master) replica
        # as part of PrePrepare 3PC batch
        ledger_id, seq_no = self.db_manager.get_store(SEQ_NO_DB_LABEL).get_by_payload_digest(request.payload_digest)
        if ledger_id is not None and seq_no is not None:
            raise SuspiciousPrePrepare('Trying to order already ordered request')

        ledger = self.db_manager.get_ledger(self._ledger_id_for_request(request))
        for txn in ledger.uncommittedTxns:
            if get_payload_digest(txn) == request.payload_digest:
                raise SuspiciousPrePrepare('Trying to order already ordered request')

        # TAA validation
        # For now, we need to call taa_validation not from dynamic_validation because
        # req_pp_time is required
        self._write_manager.do_taa_validation(request, req_pp_time, self._config)
        self._write_manager.dynamic_validation(request, req_pp_time)

    @measure_consensus_time(MetricsName.REQUEST_PROCESSING_TIME,
                            MetricsName.BACKUP_REQUEST_PROCESSING_TIME)
    def _process_req_during_batch(self,
                                  req: Request,
                                  cons_time: int):
        """
                This method will do dynamic validation and apply requests.
                If there is any errors during validation it would be raised
                """
        if self.is_master:
            self._do_dynamic_validation(req, cons_time)
            self._write_manager.apply_request(req, cons_time)

    # ToDo: Maybe we should remove this,
    #  because we have the same one in replica's validator
    def can_send_3pc_batch(self):
        if not self._data.is_primary:
            return False
        if not self._data.is_participating:
            return False
        if not self.is_master and not self._data.master_reordered_after_vc:
            return False
        if self._data.waiting_for_new_view:
            return False
        if self._data.prev_view_prepare_cert > self._lastPrePrepareSeqNo:
            return False
        # do not send new 3PC batches in a new view until the first batch is ordered
        if self.view_no > 0 and self._lastPrePrepareSeqNo > self._data.prev_view_prepare_cert \
                and self.last_ordered_3pc[1] < self._data.prev_view_prepare_cert + 1:
            return False

        if self.view_no < self.last_ordered_3pc[0]:
            return False
        if self.view_no == self.last_ordered_3pc[0]:
            if self._lastPrePrepareSeqNo < self.last_ordered_3pc[1]:
                return False
            # This check is done for current view only to simplify logic and avoid
            # edge cases between views, especially taking into account that we need
            # to send a batch in new view as soon as possible
            if self._config.Max3PCBatchesInFlight is not None:
                batches_in_flight = self._lastPrePrepareSeqNo - self.last_ordered_3pc[1]
                if batches_in_flight >= self._config.Max3PCBatchesInFlight:
                    if self._can_log_skip_send_3pc():
                        logger.info("{} not creating new batch because there already {} in flight out of {} allowed".
                                    format(self.name, batches_in_flight, self._config.Max3PCBatchesInFlight))
                    return False

        self._skip_send_3pc_ts = None
        return True

    def _can_log_skip_send_3pc(self):
        current_time = time.perf_counter()
        if self._skip_send_3pc_ts is None:
            self._skip_send_3pc_ts = current_time
            return True

        if current_time - self._skip_send_3pc_ts > self._config.Max3PCBatchWait:
            self._skip_send_3pc_ts = current_time
            return True

        return False

    def can_order_commits(self):
        if self._data.is_participating:
            return True
        if self._data.is_synced and self._data.legacy_vc_in_progress:
            return True
        return False

    def dequeue_pre_prepares(self):
        """
        Dequeue any received PRE-PREPAREs that did not have finalized requests
        or the replica was missing any PRE-PREPAREs before it
        :return:
        """
        ppsReady = []
        # Check if any requests have become finalised belonging to any stashed
        # PRE-PREPAREs.
        for i, (pp, sender, reqIds) in enumerate(
                self.prePreparesPendingFinReqs):
            finalised = set()
            for r in reqIds:
                if self._requests.is_finalised(r):
                    finalised.add(r)
            diff = reqIds.difference(finalised)
            # All requests become finalised
            if not diff:
                ppsReady.append(i)
            self.prePreparesPendingFinReqs[i] = (pp, sender, diff)

        for i in sorted(ppsReady, reverse=True):
            pp, sender, _ = self.prePreparesPendingFinReqs.pop(i)
            self.prePreparesPendingPrevPP[pp.viewNo, pp.ppSeqNo] = (pp, sender)

        r = 0
        while self.prePreparesPendingPrevPP and self._can_dequeue_pre_prepare(
                *self.prePreparesPendingPrevPP.iloc[0]):
            _, (pp, sender) = self.prePreparesPendingPrevPP.popitem(last=False)
            if not self._can_pp_seq_no_be_in_view(pp.viewNo, pp.ppSeqNo):
                self._discard(pp, "Pre-Prepare from a previous view",
                              logger.debug)
                continue
            logger.info("{} popping stashed PREPREPARE{} "
                        "from sender {}".format(self, (pp.viewNo, pp.ppSeqNo), sender))
            self._network.process_incoming(pp, sender)
            r += 1
        return r

    def _can_dequeue_pre_prepare(self, view_no: int, pp_seq_no: int):
        return self._is_next_pre_prepare(view_no, pp_seq_no) or compare_3PC_keys(
            (view_no, pp_seq_no), self.last_ordered_3pc) >= 0

    # TODO: Convert this into a free function?
    def _discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        """
        Discard a message and log a reason using the specified `logMethod`.

        :param msg: the message to discard
        :param reason: the reason why this message is being discarded
        :param logMethod: the logging function to be used
        :param cliOutput: if truthy, informs a CLI that the logged msg should
        be printed
        """
        reason = "" if not reason else " because {}".format(reason)
        logMethod("{} discarding message {}{}".format(self, msg, reason),
                  extra={"cli": cliOutput})

    def _can_pp_seq_no_be_in_view(self, view_no, pp_seq_no):
        """
        Checks if the `pp_seq_no` could have been in view `view_no`. It will
        return False when the `pp_seq_no` belongs to a later view than
        `view_no` else will return True
        :return:
        """
        if view_no > self.view_no:
            raise PlenumValueError(
                'view_no', view_no,
                "<= current view_no {}".format(self.view_no),
                prefix=self
            )

        return view_no == self.view_no or (view_no < self.view_no and self._data.legacy_last_prepared_before_view_change and
                                           compare_3PC_keys((view_no, pp_seq_no),
                                                            self._data.legacy_last_prepared_before_view_change) >= 0)

    def send_3pc_batch(self):
        if not self.can_send_3pc_batch():
            return 0

        sent_batches = set()

        # 1. send 3PC batches with requests for every ledger
        self._send_3pc_batches_for_ledgers(sent_batches)

        # 2. for every ledger we haven't just sent a 3PC batch check if it's not fresh enough,
        # and send an empty 3PC batch to update the state if needed
        self._send_3pc_freshness_batch(sent_batches)

        # 3. send 3PC batch if new primaries elected
        self.l_send_3pc_primaries_batch(sent_batches)

        # 4. update ts of last sent 3PC batch
        if len(sent_batches) > 0:
            self.lastBatchCreated = self.get_current_time()

        return len(sent_batches)

    def l_send_3pc_primaries_batch(self, sent_batches):
        # As we've selected new primaries, we need to send 3pc batch,
        # so this primaries can be saved in audit ledger
        if not sent_batches and self.primaries_batch_needed:
            logger.debug("Sending a 3PC batch to propagate newly selected primaries")
            self.primaries_batch_needed = False
            sent_batches.add(self._do_send_3pc_batch(ledger_id=DOMAIN_LEDGER_ID))

    def _send_3pc_freshness_batch(self, sent_batches):
        if not self._config.UPDATE_STATE_FRESHNESS:
            return

        if not self.is_master:
            return

        # Update freshness for all outdated ledgers sequentially without any waits
        # TODO: Consider sending every next update in Max3PCBatchWait only
        outdated_ledgers = self._freshness_checker.check_freshness(self.get_time_for_3pc_batch())
        for ledger_id, ts in outdated_ledgers.items():
            if ledger_id in sent_batches:
                logger.debug("Ledger {} is not updated for {} seconds, "
                             "but a 3PC for this ledger has been just sent".format(ledger_id, ts))
                continue

            logger.info("Ledger {} is not updated for {} seconds, "
                        "so its freshness state is going to be updated now".format(ledger_id, ts))
            sent_batches.add(
                self._do_send_3pc_batch(ledger_id=ledger_id))

    def _send_3pc_batches_for_ledgers(self, sent_batches):
        # TODO: Consider sending every next update in Max3PCBatchWait only
        for ledger_id, q in self.requestQueues.items():
            if len(q) == 0:
                continue

            queue_full = len(q) >= self._config.Max3PCBatchSize
            timeout = self.lastBatchCreated + self._config.Max3PCBatchWait < self.get_current_time()
            if not queue_full and not timeout:
                continue

            sent_batches.add(
                self._do_send_3pc_batch(ledger_id=ledger_id))

    def _do_send_3pc_batch(self, ledger_id):
        oldStateRootHash = self.get_state_root_hash(ledger_id, to_str=False)
        pre_prepare = self.create_3pc_batch(ledger_id)
        self.send_pre_prepare(pre_prepare)
        if not self.is_master:
            self.db_manager.get_store(LAST_SENT_PP_STORE_LABEL).store_last_sent_pp_seq_no(
                self._data.inst_id, pre_prepare.ppSeqNo)
        self._track_batches(pre_prepare, oldStateRootHash)
        return ledger_id

    @measure_consensus_time(MetricsName.CREATE_3PC_BATCH_TIME,
                            MetricsName.BACKUP_CREATE_3PC_BATCH_TIME)
    def create_3pc_batch(self, ledger_id):
        pp_seq_no = self.lastPrePrepareSeqNo + 1
        pool_state_root_hash = self.get_state_root_hash(POOL_LEDGER_ID)
        logger.debug("{} creating batch {} for ledger {} with state root {}".format(
            self, pp_seq_no, ledger_id,
            self.get_state_root_hash(ledger_id, to_str=False)))

        if self.last_accepted_pre_prepare_time is None:
            last_ordered_ts = self._get_last_timestamp_from_state(ledger_id)
            if last_ordered_ts:
                self.last_accepted_pre_prepare_time = last_ordered_ts

        # DO NOT REMOVE `view_no` argument, used while replay
        # tm = self.utc_epoch
        tm = self._get_utc_epoch_for_preprepare(self._data.inst_id, self.view_no,
                                                pp_seq_no)

        reqs, invalid_indices, rejects = self._consume_req_queue_for_pre_prepare(
            ledger_id, tm, self.view_no, pp_seq_no)

        req_ids = [req.digest for req in reqs]
        digest = self.generate_pp_digest(req_ids, self.view_no, tm)
        if self.is_master:
            three_pc_batch = ThreePcBatch(
                ledger_id=ledger_id,
                inst_id=self._data.inst_id,
                view_no=self.view_no,
                pp_seq_no=pp_seq_no,
                pp_time=tm,
                state_root=self.get_state_root_hash(ledger_id, to_str=False),
                txn_root=self.get_txn_root_hash(ledger_id, to_str=False),
                valid_digests=self._get_valid_req_ids_from_all_requests(reqs, invalid_indices),
                pp_digest=digest,
                original_view_no=self.view_no,
            )
            self.post_batch_creation(three_pc_batch)

        state_root_hash = self.get_state_root_hash(ledger_id)
        audit_txn_root_hash = self.get_txn_root_hash(AUDIT_LEDGER_ID)

        # TODO: for now default value for fields sub_seq_no is 0 and for final is True
        params = [
            self._data.inst_id,
            self.view_no,
            pp_seq_no,
            tm,
            req_ids,
            invalid_index_serializer.serialize(invalid_indices, toBytes=False),
            digest,
            ledger_id,
            state_root_hash,
            self.get_txn_root_hash(ledger_id),
            0,
            True,
            pool_state_root_hash,
            audit_txn_root_hash,
        ]

        # BLS multi-sig:
        params = self.l_bls_bft_replica.update_pre_prepare(params, ledger_id)

        pre_prepare = PrePrepare(*params)

        logger.trace('{} created a PRE-PREPARE with {} requests for ledger {}'.format(self, len(reqs), ledger_id))
        self.last_accepted_pre_prepare_time = tm
        if self.is_master and rejects:
            for reject in rejects:
                self._network.send(reject)

        self._add_to_sent_pre_prepares(pre_prepare)
        return pre_prepare

    def _get_last_timestamp_from_state(self, ledger_id):
        if ledger_id == DOMAIN_LEDGER_ID:
            ts_store = self.db_manager.get_store(TS_LABEL)
            if ts_store:
                last_timestamp = ts_store.get_last_key()
                if last_timestamp:
                    last_timestamp = int(last_timestamp.decode())
                    logger.debug("Last ordered timestamp from store is : {}".format(last_timestamp))
                    return last_timestamp
        return None

    # This is to enable replaying, inst_id, view_no and pp_seq_no are used
    # while replaying
    def _get_utc_epoch_for_preprepare(self, inst_id, view_no, pp_seq_no):
        tm = self.get_time_for_3pc_batch()
        if self.last_accepted_pre_prepare_time and \
                tm < self.last_accepted_pre_prepare_time:
            tm = self.last_accepted_pre_prepare_time
        return tm

    def _consume_req_queue_for_pre_prepare(self, ledger_id, tm,
                                           view_no, pp_seq_no):
        reqs = []
        rejects = []
        invalid_indices = []
        idx = 0
        while len(reqs) < self._config.Max3PCBatchSize \
                and self.requestQueues[ledger_id]:
            key = self.requestQueues[ledger_id].pop(0)
            if key in self._requests:
                fin_req = self._requests[key].finalised
                malicious_req = False
                try:
                    self._process_req_during_batch(fin_req,
                                                   tm)

                except (
                        InvalidClientMessageException,
                        UnknownIdentifier
                ) as ex:
                    logger.warning('{} encountered exception {} while processing {}, '
                                   'will reject'.format(self, ex, fin_req))
                    rejects.append((fin_req.key, Reject(fin_req.identifier, fin_req.reqId, ex)))
                    invalid_indices.append(idx)
                except SuspiciousPrePrepare:
                    malicious_req = True
                finally:
                    if not malicious_req:
                        reqs.append(fin_req)
                if not malicious_req:
                    idx += 1
            else:
                logger.debug('{} found {} in its request queue but the '
                             'corresponding request was removed'.format(self, key))

        return reqs, invalid_indices, rejects

    @measure_consensus_time(MetricsName.SEND_PREPREPARE_TIME,
                            MetricsName.BACKUP_SEND_PREPREPARE_TIME)
    def send_pre_prepare(self, ppReq: PrePrepare):
        key = (ppReq.viewNo, ppReq.ppSeqNo)
        logger.debug("{} sending PRE-PREPARE{}".format(self, key))
        self._send(ppReq, stat=TPCStat.PrePrepareSent)

    def _send(self, msg, dst=None, stat=None) -> None:
        """
        Send a message to the node on which this replica resides.

        :param stat:
        :param rid: remote id of one recipient (sends to all recipients if None)
        :param msg: the message to send
        """
        if stat:
            self.stats.inc(stat)
        self._network.send(msg, dst=dst)

    def revert_unordered_batches(self):
        """
        Revert changes to ledger (uncommitted) and state made by any requests
        that have not been ordered.
        """
        i = 0
        for key in sorted(self.batches.keys(), reverse=True):
            if compare_3PC_keys(self.last_ordered_3pc, key) > 0:
                ledger_id, discarded, _, prevStateRoot, len_reqIdr = self.batches.pop(key)
                discarded = invalid_index_serializer.deserialize(discarded)
                logger.debug('{} reverting 3PC key {}'.format(self, key))
                self._revert(ledger_id, prevStateRoot, len_reqIdr - len(discarded))
                pre_prepare = self.get_preprepare(*key)
                if pre_prepare:
                    for req_id in pre_prepare.reqIdr:
                        self.requestQueues[ledger_id].add(req_id)
                self._lastPrePrepareSeqNo -= 1
                i += 1
            else:
                break
        last_txn = self.db_manager.get_ledger(AUDIT_LEDGER_ID).get_last_committed_txn()
        self.last_accepted_pre_prepare_time = None if last_txn is None else get_txn_time(last_txn)
        logger.info('{} reverted {} batches before starting catch up'.format(self, i))
        return i

    def l_last_prepared_certificate_in_view(self) -> Optional[Tuple[int, int]]:
        # Pick the latest sent COMMIT in the view.
        # TODO: Consider stashed messages too?
        if not self.is_master:
            raise LogicError("{} is not a master".format(self))
        keys = []
        quorum = self._data.quorums.prepare.value
        for key in self.prepares.keys():
            if self.prepares.hasQuorum(ThreePhaseKey(*key), quorum):
                keys.append(key)
        return max_3PC_key(keys) if keys else None

    def _caught_up_till_3pc(self, last_caught_up_3PC):
        self.last_ordered_3pc = last_caught_up_3PC
        self._remove_till_caught_up_3pc(last_caught_up_3PC)

        # Get all pre-prepares and prepares since the latest stable checkpoint
        audit_ledger = self.db_manager.get_ledger(AUDIT_LEDGER_ID)
        last_txn = audit_ledger.get_last_txn()

        if not last_txn:
            return

        self._data.last_batch_timestamp = get_txn_time(last_txn)

        to = get_payload_data(last_txn)[AUDIT_TXN_PP_SEQ_NO]
        frm = to - to % self._config.CHK_FREQ + 1

        try:
            batch_ids = [
                BatchID(
                    view_no=get_payload_data(txn)[AUDIT_TXN_VIEW_NO],
                    pp_view_no=get_payload_data(txn)[AUDIT_TXN_VIEW_NO],
                    pp_seq_no=get_payload_data(txn)[AUDIT_TXN_PP_SEQ_NO],
                    pp_digest=get_payload_data(txn)[AUDIT_TXN_DIGEST]
                )
                for _, txn in audit_ledger.getAllTxn(frm=frm, to=to)
            ]

            self._data.preprepared.extend(batch_ids)
            self._data.prepared.extend(batch_ids)

        except KeyError as e:
            logger.warning(
                'Pre-Prepared/Prepared not restored as Audit TXN is missing needed fields: {}'.format(e)
            )

    def catchup_clear_for_backup(self):
        if not self._data.is_primary:
            self.sent_preprepares.clear()
            self.prePrepares.clear()
            self.prepares.clear()
            self.commits.clear()
            self._data.prepared.clear()
            self._data.preprepared.clear()
            self.first_batch_after_catchup = True

    def _remove_till_caught_up_3pc(self, last_caught_up_3PC):
        """
        Remove any 3 phase messages till the last ordered key and also remove
        any corresponding request keys
        """
        outdated_pre_prepares = {}
        for key, pp in self.prePrepares.items():
            if compare_3PC_keys(key, last_caught_up_3PC) >= 0:
                outdated_pre_prepares[key] = pp
        for key, pp in self.sent_preprepares.items():
            if compare_3PC_keys(key, last_caught_up_3PC) >= 0:
                outdated_pre_prepares[key] = pp

        logger.trace('{} going to remove messages for {} 3PC keys'.format(
            self, len(outdated_pre_prepares)))

        for key, pp in outdated_pre_prepares.items():
            self.batches.pop(key, None)
            self.sent_preprepares.pop(key, None)
            self.prePrepares.pop(key, None)
            self.prepares.pop(key, None)
            self.commits.pop(key, None)
            self._discard_ordered_req_keys(pp)
            self._clear_batch(pp)

    def get_sent_preprepare(self, viewNo, ppSeqNo):
        key = (viewNo, ppSeqNo)
        return self.sent_preprepares.get(key)

    def get_sent_prepare(self, viewNo, ppSeqNo):
        key = (viewNo, ppSeqNo)
        if key in self.prepares:
            prepare = self.prepares[key].msg
            if self.prepares.hasPrepareFrom(prepare, self.name):
                return prepare
        return None

    def get_sent_commit(self, viewNo, ppSeqNo):
        key = (viewNo, ppSeqNo)
        if key in self.commits:
            commit = self.commits[key].msg
            if self.commits.hasCommitFrom(commit, self.name):
                return commit
        return None

    @staticmethod
    def generate_pp_digest(req_digests, original_view_no, pp_time):
        return sha256(serialize_msg_for_signing([original_view_no, pp_time, *req_digests])).hexdigest()

    def replica_batch_digest(self, reqs):
        return replica_batch_digest(reqs)

    def _clear_all_3pc_msgs(self):

        # Clear the 3PC log
        self.batches.clear()
        self.prePrepares.clear()
        self.prepares.clear()
        self.commits.clear()
        self.pre_prepare_tss.clear()
        self.prePreparesPendingFinReqs.clear()
        self.prePreparesPendingPrevPP.clear()
        self.sent_preprepares.clear()

    def process_view_change_started(self, msg: ViewChangeStarted):
        # 1. update shared data
        self._data.preprepared = []
        self._data.prepared = []

        # 2. save existing PrePrepares
        self._update_old_view_preprepares(itertools.chain(self.prePrepares.values(), self.sent_preprepares.values()))

        # 3. revert unordered transactions
        if self.is_master:
            self.revert_unordered_batches()

        # 4. clear all 3pc messages
        self._clear_all_3pc_msgs()

        # 5. clear ordered from previous view
        self.ordered.clear_below_view(msg.view_no)

        return PROCESS, None

    def process_new_view_accepted(self, msg: NewViewAccepted):
        self._setup_for_non_master_after_view_change(msg.view_no)

    def _setup_for_non_master_after_view_change(self, current_view):
        if not self.is_master:
            for v in list(self.stashed_out_of_order_commits.keys()):
                if v < current_view:
                    self.stashed_out_of_order_commits.pop(v)

    def process_catchup_checkpoints_applied(self, msg: CatchupCheckpointsApplied):
        if compare_3PC_keys(msg.master_last_ordered,
                            msg.last_caught_up_3PC) > 0:
            if self.is_master:
                self._caught_up_till_3pc(msg.last_caught_up_3PC)
            else:
                self.first_batch_after_catchup = True
                self.catchup_clear_for_backup()

        self._clear_prev_view_pre_prepares()
        self._stasher.process_all_stashed(STASH_CATCH_UP)
        self._stasher.process_all_stashed(STASH_WAITING_FIRST_BATCH_IN_VIEW)
        self._finish_master_reordering()

    def _update_old_view_preprepares(self, pre_prepares: List[PrePrepare]):
        for pp in pre_prepares:
            view_no = get_original_viewno(pp)
            self.old_view_preprepares[(view_no, pp.ppSeqNo, pp.digest)] = pp

    def process_new_view_checkpoints_applied(self, msg: NewViewCheckpointsApplied):
        result, reason = self._validate(msg)
        if result != PROCESS:
            return result, reason

        logger.info("{} processing {}".format(self, msg))

        missing_batches = []
        if self.is_master:
            # apply PrePrepares from NewView that we have
            # request missing PrePrepares from NewView
            for batch_id in msg.batches:
                pp = self.old_view_preprepares.get((batch_id.pp_view_no, batch_id.pp_seq_no, batch_id.pp_digest))
                if pp is None:
                    missing_batches.append(batch_id)
                else:
                    self._process_pre_prepare_from_old_view(pp)

        if len(msg.batches) == 0:
            self._finish_master_reordering()

        if missing_batches:
            self._request_old_view_pre_prepares(missing_batches)

        self.primaries_batch_needed = True

        if not missing_batches:
            self._reapplied_in_new_view()

    def process_old_view_preprepare_request(self, msg: OldViewPrePrepareRequest, sender):
        result, reason = self._validate(msg)
        if result != PROCESS:
            return result, reason

        old_view_pps = []
        for batch_id in msg.batch_ids:
            batch_id = BatchID(*batch_id)
            pp = self.old_view_preprepares.get((batch_id.pp_view_no, batch_id.pp_seq_no, batch_id.pp_digest))
            if pp is not None:
                old_view_pps.append(pp)
        rep = OldViewPrePrepareReply(self._data.inst_id, old_view_pps)
        self._send(rep, dst=[replica_name_to_node_name(sender)])

    def process_old_view_preprepare_reply(self, msg: OldViewPrePrepareReply, sender):
        result, reason = self._validate(msg)
        if result != PROCESS:
            return result, reason

        for pp_dict in msg.preprepares:
            try:
                pp = PrePrepare(**pp_dict)
                if self._data.new_view is None or \
                        preprepare_to_batch_id(pp) not in self._data.new_view.batches:
                    logger.info("Skipped useless PrePrepare {} from {}".format(pp, sender))
                    continue
                self._process_pre_prepare_from_old_view(pp)
            except Exception as ex:
                # TODO: catch more specific error here
                logger.error("Invalid PrePrepare in {}: {}".format(msg, ex))
        if self._data.prev_view_prepare_cert and self._data.prev_view_prepare_cert <= self.lastPrePrepareSeqNo:
            self._reapplied_in_new_view()

    def _request_old_view_pre_prepares(self, batches):
        old_pp_req = OldViewPrePrepareRequest(self._data.inst_id, batches)
        self._send(old_pp_req)

    def _process_pre_prepare_from_old_view(self, pp):
        new_pp = updateNamedTuple(pp, viewNo=self.view_no, originalViewNo=get_original_viewno(pp))

        # PrePrepare is accepted from the current Primary only
        sender = generateName(self._data.primary_name, self._data.inst_id)
        self.process_preprepare(new_pp, sender)

        return PROCESS, None

    def _reapplied_in_new_view(self):
        self._stasher.process_all_stashed(STASH_VIEW_3PC)
        self._bus.send(ReAppliedInNewView())

    def process_checkpoint_stabilized(self, msg: CheckpointStabilized):
        self.gc(msg.last_stable_3pc)

    def _preprepare_batch(self, pp: PrePrepare):
        """
        After pp had validated, it placed into _preprepared list
        """
        batch_id = preprepare_to_batch_id(pp)
        if batch_id not in self._data.preprepared:
            self._data.preprepared.append(batch_id)

    def _prepare_batch(self, pp: PrePrepare):
        """
        After prepared certificate for pp had collected,
        it removed from _preprepared and placed into _prepared list
        """
        batch_id = preprepare_to_batch_id(pp)
        if batch_id not in self._data.prepared:
            self._data.prepared.append(batch_id)

    def _clear_batch(self, pp: PrePrepare):
        """
        When 3pc batch processed, it removed from _prepared list
        """
        batch_id = preprepare_to_batch_id(pp)
        if batch_id in self._data.preprepared:
            self._data.preprepared.remove(batch_id)
        if batch_id in self._data.prepared:
            self._data.prepared.remove(batch_id)

    def _finish_master_reordering(self):
        if not self.is_master:
            self._data.master_reordered_after_vc = True
