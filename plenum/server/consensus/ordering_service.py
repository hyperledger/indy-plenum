import itertools
import time
from collections import defaultdict, OrderedDict, deque
from functools import partial
from typing import Tuple, List, Set, Optional, Dict, Iterable

import math
from orderedset._orderedset import OrderedSet
from sortedcontainers import SortedList

from common.serializers.serialization import state_roots_serializer, invalid_index_serializer
from crypto.bls.bls_bft_replica import BlsBftReplica
from plenum.common.config_util import getConfig
from plenum.common.constants import POOL_LEDGER_ID, SEQ_NO_DB_LABEL, ReplicaHooks, AUDIT_LEDGER_ID, TXN_TYPE, \
    LAST_SENT_PP_STORE_LABEL, AUDIT_TXN_PP_SEQ_NO, AUDIT_TXN_VIEW_NO, AUDIT_TXN_PRIMARIES, PREPREPARE, PREPARE, COMMIT
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.exceptions import SuspiciousNode, InvalidClientMessageException, SuspiciousPrePrepare, \
    UnknownIdentifier
from plenum.common.ledger import Ledger
from plenum.common.messages.internal_messages import HookMessage, OutboxMessage, DoCheckpointMessage, \
    RemoveStashedCheckpoints, RequestPropagates
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit, Reject, ThreePhaseKey, Ordered, \
    CheckpointState, MessageReq
from plenum.common.metrics_collector import MetricsName
from plenum.common.request import Request
from plenum.common.stashing_router import StashingRouter
from plenum.common.timer import TimerService, RepeatingTimer
from plenum.common.txn_util import get_payload_digest, get_payload_data, get_seq_no
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys, updateNamedTuple, SortedDict, getMaxFailures, mostCommonElement, \
    get_utc_epoch
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.msg_validator import ThreePCMsgValidator
from plenum.server.models import Prepares, Commits
from plenum.server.propagator import Requests
from plenum.server.replica import PP_APPLY_REJECT_WRONG, PP_APPLY_WRONG_DIGEST, PP_APPLY_WRONG_STATE, \
    PP_APPLY_ROOT_HASH_MISMATCH, PP_APPLY_HOOK_ERROR, PP_SUB_SEQ_NO_WRONG, PP_NOT_FINAL, PP_APPLY_AUDIT_HASH_MISMATCH, \
    PP_REQUEST_ALREADY_ORDERED, PP_CHECK_NOT_FROM_PRIMARY, PP_CHECK_TO_PRIMARY, PP_CHECK_DUPLICATE, \
    PP_CHECK_INCORRECT_POOL_STATE_ROOT, PP_CHECK_OLD, PP_CHECK_REQUEST_NOT_FINALIZED, PP_CHECK_NOT_NEXT, \
    PP_CHECK_WRONG_TIME, Replica, TPCStat, Stats, ConsensusDataHelper, OrderedTracker
from plenum.server.replica_freshness_checker import FreshnessChecker
from plenum.server.replica_validator_enums import INCORRECT_INSTANCE, DISCARD, INCORRECT_PP_SEQ_NO, ALREADY_ORDERED, \
    STASH_VIEW, FUTURE_VIEW, OLD_VIEW, PROCESS, GREATER_PREP_CERT, STASH_CATCH_UP, CATCHING_UP, OUTSIDE_WATERMARKS, \
    STASH_WATERMARKS
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from plenum.server.suspicion_codes import Suspicions
from stp_core.common.log import getlogger


class OrderingService:

    def __init__(self,
                 data: ConsensusSharedData,
                 timer: TimerService,
                 bus: InternalBus,
                 network: ExternalBus,
                 write_manager: WriteRequestManager,
                 bls_bft_replica: BlsBftReplica,
                 is_master=True,
                 get_current_time=None,
                 get_time_for_3pc_batch=None,
                 stasher=None):
        self._data = data
        self._requests = self._data.requests
        self._timer = timer
        self._bus = bus
        self._network = network
        self._write_manager = write_manager
        self._is_master = is_master
        self._name = self._data.name
        self.get_time_for_3pc_batch = get_time_for_3pc_batch or get_utc_epoch

        self._config = getConfig()
        self._logger = getlogger()
        self._stasher = stasher if stasher else StashingRouter(self._config.REPLICA_STASH_LIMIT)
        self._validator = ThreePCMsgValidator(self._data)
        self.get_current_time = get_current_time or self._timer.get_current_time
        self._out_of_order_repeater = RepeatingTimer(self._timer,
                                                     self._config.PROCESS_STASHED_OUT_OF_ORDER_COMMITS_INTERVAL,
                                                     self.l_process_stashed_out_of_order_commits,
                                                     active=False)

        """
        Maps from legacy replica code
        """
        self._state_root_serializer = state_roots_serializer
        self.pre_prepares_stashed_for_incorrect_time = {}
        self.legacy_preprepares = SortedDict(lambda k: (k[0], k[1]))
        self.last_accepted_pre_prepare_time = None
        # Tracks for which keys PRE-PREPAREs have been requested.
        # Cleared in `gc`
        # type: Dict[Tuple[int, int], Optional[Tuple[str, str, str]]]
        self.requested_pre_prepares = {}

        # Tracks for which keys PREPAREs have been requested.
        # Cleared in `gc`
        # type: Dict[Tuple[int, int], Optional[Tuple[str, str, str]]]
        self.requested_prepares = {}

        # Tracks for which keys COMMITs have been requested.
        # Cleared in `gc`
        # type: Dict[Tuple[int, int], Optional[Tuple[str, str, str]]]
        self.requested_commits = {}

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

        self._lastPrePrepareSeqNo = self._data.low_watermark

        # COMMITs that are stored for which there are no PRE-PREPARE or PREPARE
        # received
        self.commitsWaitingForPrepare = {}
        # type: Dict[Tuple[int, int], deque]

        # Dictionary of sent PRE-PREPARE that are stored by primary replica
        # which it has broadcasted to all other non primary replicas
        # Key of dictionary is a 2 element tuple with elements viewNo,
        # pre-prepare seqNo and value is the received PRE-PREPARE
        self.sentPrePrepares = SortedDict(lambda k: (k[0], k[1]))
        # type: Dict[Tuple[int, int], PrePrepare]

        # Dictionary of received PRE-PREPAREs. Key of dictionary is a 2
        # element tuple with elements viewNo, pre-prepare seqNo and value
        # is the received PRE-PREPARE
        self.prePrepares = SortedDict(lambda k: (k[0], k[1]))
        # type: Dict[Tuple[int, int], PrePrepare]

        # Dictionary of received Prepare requests. Key of dictionary is a 2
        # element tuple with elements viewNo, seqNo and value is a 2 element
        # tuple containing request digest and set of sender node names(sender
        # replica names in case of multiple protocol instances)
        # (viewNo, seqNo) -> ((identifier, reqId), {senders})
        self.prepares = Prepares()
        # type: Dict[Tuple[int, int], Tuple[Tuple[str, int], Set[str]]]

        self.commits = Commits()
        # type: Dict[Tuple[int, int], Tuple[Tuple[str, int], Set[str]]]

        # Dictionary to keep track of the which replica was primary during each
        # view. Key is the view no and value is the name of the primary
        # replica during that view
        self.primary_names = OrderedDict()  # type: OrderedDict[int, str]

        # Indicates name of the primary replica of this protocol instance.
        # None in case the replica does not know who the primary of the
        # instance is
        self._primary_name = None  # type: Optional[str]

        # Did we log a message about getting request while absence of primary
        self.warned_no_primary = False

        # Queues used in PRE-PREPARE for each ledger,
        self.requestQueues = {}  # type: Dict[int, OrderedSet]

        self.batches = OrderedDict()  # type: OrderedDict[Tuple[int, int]]

        self.stats = Stats(TPCStat)

        self.l_batches = OrderedDict()  # type: OrderedDict[Tuple[int, int]]

        self.l_bls_bft_replica = bls_bft_replica

        # Set of tuples to keep track of ordered requests. Each tuple is
        # (viewNo, ppSeqNo).
        self.ordered = OrderedTracker()

        # Commits which are not being ordered since commits with lower
        # sequence numbers have not been ordered yet. Key is the
        # viewNo and value a map of pre-prepare sequence number to commit
        # type: Dict[int,Dict[int,Commit]]
        self.stashed_out_of_order_commits = {}

        self._freshness_checker = FreshnessChecker(freshness_timeout=self._config.STATE_FRESHNESS_UPDATE_INTERVAL)
        self._skip_send_3pc_ts = None

        self._consensus_data_helper = ConsensusDataHelper(self._data)

        self._stasher.subscribe(PrePrepare, self.process_preprepare)
        self._stasher.subscribe(Prepare, self.process_prepare)
        self._stasher.subscribe(Commit, self.process_commit)
        self._stasher.subscribe_to(network)

    def process_prepare(self, prepare: Prepare, sender: str):
        """
        Validate and process the PREPARE specified.
        If validation is successful, create a COMMIT and broadcast it.

        :param prepare: a PREPARE msg
        :param sender: name of the node that sent the PREPARE
        """
        result, reason = self._validate(prepare)
        if result != PROCESS:
            return result

        key = (prepare.viewNo, prepare.ppSeqNo)
        self._logger.debug("{} received PREPARE{} from {}".format(self, key, sender))

        # TODO move this try/except up higher
        try:
            if self.l_validatePrepare(prepare, sender):
                self.l_addToPrepares(prepare, sender)
                self.stats.inc(TPCStat.PrepareRcvd)
                self._logger.debug("{} processed incoming PREPARE {}".format(
                    self, (prepare.viewNo, prepare.ppSeqNo)))
            else:
                # TODO let's have isValidPrepare throw an exception that gets
                # handled and possibly logged higher
                self._logger.trace("{} cannot process incoming PREPARE".format(self))
        except SuspiciousNode as ex:
            self.report_suspicious_node(ex)

    def l_validatePrepare(self, prepare: Prepare, sender: str) -> bool:
        """
        Return whether the PREPARE specified is valid.

        :param prepare: the PREPARE to validate
        :param sender: the name of the node that sent the PREPARE
        :return: True if PREPARE is valid, False otherwise
        """
        key = (prepare.viewNo, prepare.ppSeqNo)
        primaryStatus = self.l_isPrimaryForMsg(prepare)

        ppReq = self.l_getPrePrepare(*key)

        # If a non primary replica and receiving a PREPARE request before a
        # PRE-PREPARE request, then proceed

        # PREPARE should not be sent from primary
        if self.l_isMsgFromPrimary(prepare, sender):
            raise SuspiciousNode(sender, Suspicions.PR_FRM_PRIMARY, prepare)

        # If non primary replica
        if primaryStatus is False:
            if self.prepares.hasPrepareFrom(prepare, sender):
                raise SuspiciousNode(
                    sender, Suspicions.DUPLICATE_PR_SENT, prepare)
            # If PRE-PREPARE not received for the PREPARE, might be slow
            # network
            if not ppReq:
                self.l_enqueue_prepare(prepare, sender)
                self.l_setup_last_ordered_for_non_master()
                return False
        # If primary replica
        if primaryStatus is True:
            if self.prepares.hasPrepareFrom(prepare, sender):
                raise SuspiciousNode(
                    sender, Suspicions.DUPLICATE_PR_SENT, prepare)
            # If PRE-PREPARE was not sent for this PREPARE, certainly
            # malicious behavior
            elif not ppReq:
                raise SuspiciousNode(
                    sender, Suspicions.UNKNOWN_PR_SENT, prepare)

        if primaryStatus is None and not ppReq:
            self.l_enqueue_prepare(prepare, sender)
            self.l_setup_last_ordered_for_non_master()
            return False

        if prepare.digest != ppReq.digest:
            raise SuspiciousNode(sender, Suspicions.PR_DIGEST_WRONG, prepare)
        elif prepare.stateRootHash != ppReq.stateRootHash:
            raise SuspiciousNode(sender, Suspicions.PR_STATE_WRONG,
                                 prepare)
        elif prepare.txnRootHash != ppReq.txnRootHash:
            raise SuspiciousNode(sender, Suspicions.PR_TXN_WRONG,
                                 prepare)
        elif prepare.auditTxnRootHash != ppReq.auditTxnRootHash:
            raise SuspiciousNode(sender, Suspicions.PR_AUDIT_TXN_ROOT_HASH_WRONG,
                                 prepare)

        # BLS multi-sig:
        self.l_bls_bft_replica.validate_prepare(prepare, sender)

        return True

    """Method from legacy code"""
    def l_enqueue_prepare(self, pMsg: Prepare, sender: str):
        key = (pMsg.viewNo, pMsg.ppSeqNo)
        self._logger.debug("{} queueing prepare due to unavailability of PRE-PREPARE. "
                           "Prepare {} for key {} from {}".format(self, pMsg, key, sender))
        if key not in self.preparesWaitingForPrePrepare:
            self.preparesWaitingForPrePrepare[key] = deque()
        self.preparesWaitingForPrePrepare[key].append((pMsg, sender))
        if key not in self.pre_prepares_stashed_for_incorrect_time:
            if self.is_master or self.last_ordered_3pc[1] != 0:
                self.l_request_pre_prepare_for_prepare(key)
        else:
            self.l_process_stashed_pre_prepare_for_time_if_possible(key)

    def l_process_stashed_pre_prepare_for_time_if_possible(
            self, key: Tuple[int, int]):
        """
        Check if any PRE-PREPAREs that were stashed since their time was not
        acceptable, can now be accepted since enough PREPAREs are received
        """
        self._logger.debug('{} going to process stashed PRE-PREPAREs with '
                           'incorrect times'.format(self))
        q = self._data.quorums.f
        if len(self.preparesWaitingForPrePrepare[key]) > q:
            times = [pr.ppTime for (pr, _) in
                     self.preparesWaitingForPrePrepare[key]]
            most_common_time, freq = mostCommonElement(times)
            if self._data.quorums.timestamp.is_reached(freq):
                self._logger.debug('{} found sufficient PREPAREs for the '
                                   'PRE-PREPARE{}'.format(self, key))
                stashed_pp = self.pre_prepares_stashed_for_incorrect_time
                pp, sender, done = stashed_pp[key]
                if done:
                    self._logger.debug('{} already processed PRE-PREPARE{}'.format(self, key))
                    return True
                # True is set since that will indicate to `is_pre_prepare_time_acceptable`
                # that sufficient PREPAREs are received
                stashed_pp[key] = (pp, sender, True)
                self.process_preprepare(pp, sender)
                return True
        return False

    """Method from legacy code"""
    def l_request_pre_prepare_for_prepare(self, three_pc_key) -> bool:
        """
        Check if has an acceptable PRE_PREPARE already stashed, if not then
        check count of PREPAREs, make sure >f consistent PREPAREs are found,
        store the acceptable PREPARE state (digest, roots) for verification of
        the received PRE-PREPARE
        """

        if three_pc_key in self.prePreparesPendingPrevPP:
            self._logger.debug('{} not requesting a PRE-PREPARE since already found '
                               'stashed for {}'.format(self, three_pc_key))
            return False

        if len(
                self.preparesWaitingForPrePrepare[three_pc_key]) < self._data.quorums.prepare.value:
            self._logger.debug(
                '{} not requesting a PRE-PREPARE because does not have'
                ' sufficient PREPAREs for {}'.format(
                    self, three_pc_key))
            return False

        digest, state_root, txn_root, _ = \
            self.l_get_acceptable_stashed_prepare_state(three_pc_key)

        # Choose a better data structure for `prePreparesPendingFinReqs`
        pre_prepares = [pp for pp, _, _ in self.prePreparesPendingFinReqs
                        if (pp.viewNo, pp.ppSeqNo) == three_pc_key]
        if pre_prepares:
            if [pp for pp in pre_prepares if (pp.digest, pp.stateRootHash, pp.txnRootHash) == (digest, state_root, txn_root)]:
                self._logger.debug('{} not requesting a PRE-PREPARE since already '
                                   'found stashed for {}'.format(self, three_pc_key))
                return False

        self._request_pre_prepare(three_pc_key,
                                  stash_data=(digest, state_root, txn_root))
        return True

    """Method from legacy code"""
    def l_get_acceptable_stashed_prepare_state(self, three_pc_key):
        prepares = {s: (m.digest, m.stateRootHash, m.txnRootHash) for m, s in
                    self.preparesWaitingForPrePrepare[three_pc_key]}
        acceptable, freq = mostCommonElement(prepares.values())
        return (*acceptable, {s for s, state in prepares.items()
                              if state == acceptable})

    """Method from legacy code"""
    def l_isPrimaryForMsg(self, msg) -> Optional[bool]:
        """
        Return whether this replica is primary if the request's view number is
        equal this replica's view number and primary has been selected for
        the current view.
        Return None otherwise.
        :param msg: message
        """
        return self._data.is_primary if self.l_isMsgForCurrentView(msg) \
            else self.l_is_primary_in_view(msg.viewNo)

    """Method from legacy code"""
    def l_is_primary_in_view(self, viewNo: int) -> Optional[bool]:
        """
        Return whether this replica was primary in the given view
        """
        if viewNo not in self.primary_names:
            return False
        return self.primary_names[viewNo] == self.name

    def process_commit(self, commit: Commit, sender: str):
        """
        Validate and process the COMMIT specified.
        If validation is successful, return the message to the node.

        :param commit: an incoming COMMIT message
        :param sender: name of the node that sent the COMMIT
        """
        result, reason = self._validate(commit)
        if result != PROCESS:
            return result

        self._logger.debug("{} received COMMIT{} from {}".format(
            self, (commit.viewNo, commit.ppSeqNo), sender))

        if self.l_validateCommit(commit, sender):
            self.stats.inc(TPCStat.CommitRcvd)
            self.l_addToCommits(commit, sender)
            self._logger.debug("{} processed incoming COMMIT{}".format(
                self, (commit.viewNo, commit.ppSeqNo)))
        return result

    """Method from legacy code"""
    def l_validateCommit(self, commit: Commit, sender: str) -> bool:
        """
        Return whether the COMMIT specified is valid.

        :param commit: the COMMIT to validate
        :return: True if `request` is valid, False otherwise
        """
        key = (commit.viewNo, commit.ppSeqNo)
        if not self.l_has_prepared(key):
            self.l_enqueue_commit(commit, sender)
            return False

        if self.commits.hasCommitFrom(commit, sender):
            raise SuspiciousNode(sender, Suspicions.DUPLICATE_CM_SENT, commit)

        # BLS multi-sig:
        pre_prepare = self.l_getPrePrepare(commit.viewNo, commit.ppSeqNo)
        why_not = self.l_bls_bft_replica.validate_commit(commit, sender, pre_prepare)

        if why_not == BlsBftReplica.CM_BLS_SIG_WRONG:
            self._logger.warning("{} discard Commit message from "
                                 "{}:{}".format(self, sender, commit))
            raise SuspiciousNode(sender,
                                 Suspicions.CM_BLS_SIG_WRONG,
                                 commit)
        elif why_not is not None:
            self._logger.warning("Unknown error code returned for bls commit "
                                 "validation {}".format(why_not))

        return True

    def l_enqueue_commit(self, request: Commit, sender: str):
        key = (request.viewNo, request.ppSeqNo)
        self._logger.debug("{} - Queueing commit due to unavailability of PREPARE. "
                           "Request {} with key {} from {}".format(self, request, key, sender))
        if key not in self.commitsWaitingForPrepare:
            self.commitsWaitingForPrepare[key] = deque()
        self.commitsWaitingForPrepare[key].append((request, sender))

    def process_preprepare(self, pre_prepare: PrePrepare, sender: str):
        """
        Validate and process provided PRE-PREPARE, create and
        broadcast PREPARE for it.

        :param pre_prepare: message
        :param sender: name of the node that sent this message
        """
        pp_key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
        # the same PrePrepare might come here multiple times
        if (pp_key and (pre_prepare, sender) not in self.pre_prepare_tss[pp_key]):
            # TODO more clean solution would be to set timestamps
            # earlier (e.g. in zstack)
            self.pre_prepare_tss[pp_key][pre_prepare, sender] = self.get_time_for_3pc_batch()

        result, reason = self._validate(pre_prepare)
        if result != PROCESS:
            return result

        key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
        self._logger.debug("{} received PRE-PREPARE{} from {}".format(self, key, sender))

        # TODO: should we still do it?
        # Converting each req_idrs from list to tuple
        req_idrs = {f.REQ_IDR.nm: [key for key in pre_prepare.reqIdr]}
        pre_prepare = updateNamedTuple(pre_prepare, **req_idrs)

        def report_suspicious(reason):
            ex = SuspiciousNode(sender, reason, pre_prepare)
            self.report_suspicious_node(ex)

        why_not = self.l_can_process_pre_prepare(pre_prepare, sender)
        if why_not is None:
            why_not_applied = \
                self.l_process_valid_preprepare(pre_prepare, sender)
            if why_not_applied is not None:
                if why_not_applied == PP_APPLY_REJECT_WRONG:
                    report_suspicious(Suspicions.PPR_REJECT_WRONG)
                elif why_not_applied == PP_APPLY_WRONG_DIGEST:
                    report_suspicious(Suspicions.PPR_DIGEST_WRONG)
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
                    return
                elif why_not_applied == PP_APPLY_AUDIT_HASH_MISMATCH:
                    report_suspicious(Suspicions.PPR_AUDIT_TXN_ROOT_HASH_WRONG)
                elif why_not_applied == PP_REQUEST_ALREADY_ORDERED:
                    report_suspicious(Suspicions.PPR_WITH_ORDERED_REQUEST)
        elif why_not == PP_CHECK_NOT_FROM_PRIMARY:
            report_suspicious(Suspicions.PPR_FRM_NON_PRIMARY)
        elif why_not == PP_CHECK_TO_PRIMARY:
            report_suspicious(Suspicions.PPR_TO_PRIMARY)
        elif why_not == PP_CHECK_DUPLICATE:
            report_suspicious(Suspicions.DUPLICATE_PPR_SENT)
        elif why_not == PP_CHECK_INCORRECT_POOL_STATE_ROOT:
            report_suspicious(Suspicions.PPR_POOL_STATE_ROOT_HASH_WRONG)
        elif why_not == PP_CHECK_OLD:
            self._logger.info("PRE-PREPARE {} has ppSeqNo lower "
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
            self._logger.warning(
                "{} found requests in the incoming pp, of {} ledger, that are not finalized. "
                "{} of them don't have propagates: {}."
                "{} of them don't have enough propagates: {}.".format(self, pre_prepare.ledgerId,
                                                                      len(absents), absent_str,
                                                                      len(non_fin), non_fin_str))

            def signal_suspicious(req):
                self._logger.info("Request digest {} already ordered. Discard {} "
                                  "from {}".format(req, pre_prepare, sender))
                report_suspicious(Suspicions.PPR_WITH_ORDERED_REQUEST)

            # checking for payload digest is more effective
            for payload_key in non_fin_payload:
                if self.db_manager.get_store(SEQ_NO_DB_LABEL).get_by_payload_digest(payload_key) != (None, None):
                    signal_suspicious(payload_key)
                    return

            # for absents we can only check full digest
            for full_key in absents:
                if self.db_manager.get_store(SEQ_NO_DB_LABEL).get_by_full_digest(full_key) is not None:
                    signal_suspicious(full_key)
                    return

            bad_reqs = absents | non_fin
            self.l_enqueue_pre_prepare(pre_prepare, sender, bad_reqs)
            # TODO: An optimisation might be to not request PROPAGATEs
            # if some PROPAGATEs are present or a client request is
            # present and sufficient PREPAREs and PRE-PREPARE are present,
            # then the digest can be compared but this is expensive as the
            # PREPARE and PRE-PREPARE contain a combined digest
            self._schedule(partial(self.l_request_propagates_if_needed, bad_reqs, pre_prepare),
                           self._config.PROPAGATE_REQUEST_DELAY)
        elif why_not == PP_CHECK_NOT_NEXT:
            pp_view_no = pre_prepare.viewNo
            pp_seq_no = pre_prepare.ppSeqNo
            last_pp_view_no, last_pp_seq_no = self.__last_pp_3pc
            if pp_view_no >= last_pp_view_no and (
                    self.is_master or self.last_ordered_3pc[1] != 0):
                seq_frm = last_pp_seq_no + 1 if pp_view_no == last_pp_view_no else 1
                seq_to = pp_seq_no - 1
                if seq_to >= seq_frm >= pp_seq_no - self._config.CHK_FREQ + 1:
                    self._logger.warning(
                        "{} missing PRE-PREPAREs from {} to {}, "
                        "going to request".format(self, seq_frm, seq_to))
                    self.l_request_missing_three_phase_messages(
                        pp_view_no, seq_frm, seq_to)
            self.l_enqueue_pre_prepare(pre_prepare, sender)
            self.l_setup_last_ordered_for_non_master()
        elif why_not == PP_CHECK_WRONG_TIME:
            key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
            item = (pre_prepare, sender, False)
            self.pre_prepares_stashed_for_incorrect_time[key] = item
            report_suspicious(Suspicions.PPR_TIME_WRONG)
        elif why_not == BlsBftReplica.PPR_BLS_MULTISIG_WRONG:
            report_suspicious(Suspicions.PPR_BLS_MULTISIG_WRONG)
        else:
            self._logger.warning("Unknown PRE-PREPARE check status: {}".format(why_not))

    """Properties from legacy code"""

    @property
    def view_no(self):
        return self._data.view_no

    @property
    def last_ordered_3pc(self):
        return self._data.last_ordered_3pc

    @last_ordered_3pc.setter
    def last_ordered_3pc(self, lo_tuple):
        self._data.last_ordered_3pc = lo_tuple

    @property
    def l_lastPrePrepare(self):
        last_3pc = (0, 0)
        lastPp = None
        if self.sentPrePrepares:
            (v, s), pp = self.sentPrePrepares.peekitem(-1)
            last_3pc = (v, s)
            lastPp = pp
        if self.prePrepares:
            (v, s), pp = self.prePrepares.peekitem(-1)
            if compare_3PC_keys(last_3pc, (v, s)) > 0:
                lastPp = pp
        return lastPp

    @property
    def __last_pp_3pc(self):
        last_pp = self.l_lastPrePrepare
        if not last_pp:
            return self.last_ordered_3pc

        last_3pc = (last_pp.viewNo, last_pp.ppSeqNo)
        if compare_3PC_keys(self.last_ordered_3pc, last_3pc) > 0:
            return last_3pc

    @property
    def db_manager(self):
        return self._write_manager.database_manager

    @property
    def is_master(self):
        return self._is_master

    @property
    def primary_name(self):
        """
        Name of the primary replica of this replica's instance

        :return: Returns name if primary is known, None otherwise
        """
        return self._data.primary_name

    @primary_name.setter
    def primary_name(self, value: Optional[str]) -> None:
        """
        Set the value of isPrimary.

        :param value: the value to set isPrimary to
        """
        if value is not None:
            self.warned_no_primary = False
        self.primary_names[self.view_no] = value
        self.l_compact_primary_names()
        if value != self._data.primary_name:
            self._data.primary_name = value
            self._logger.info("{} setting primaryName for view no {} to: {}".
                              format(self, self.view_no, value))
            if value is None:
                # Since the GC needs to happen after a primary has been
                # decided.
                return
            self.l_gc_before_new_view()
            if self.l__should_reset_watermarks_before_new_view():
                self.l_reset_watermarks_before_new_view()

    @property
    def name(self):
        # ToDo: Change to real name
        return self._data.name

    @name.setter
    def name(self, n):
        self._data._name = n

    @property
    def f(self):
        return getMaxFailures(self._data.total_nodes)

    """Method from legacy code"""
    def l_reset_watermarks_before_new_view(self):
        # Reset any previous view watermarks since for view change to
        # successfully complete, the node must have reached the same state
        # as other nodes
        self._data.low_watermark = 0
        self._lastPrePrepareSeqNo = self._data.low_watermark

    """Method from legacy code"""
    def l__should_reset_watermarks_before_new_view(self):
        if self.view_no <= 0:
            return False
        if self.last_ordered_3pc[0] == self.view_no and self.last_ordered_3pc[1] > 0:
            return False
        return True

    """Method from legacy code"""
    def l_gc_before_new_view(self):
        # Trigger GC for all batches of old view
        # Clear any checkpoints, since they are valid only in a view
        self.l_gc(self.last_ordered_3pc)
        self._data.checkpoints.clear()
        self.l_remove_stashed_checkpoints(view_no=self.view_no)
        self._clear_prev_view_pre_prepares()

    """Method from legacy code"""
    def l_gc(self, till3PCKey):
        self._logger.info("{} cleaning up till {}".format(self, till3PCKey))
        tpcKeys = set()
        reqKeys = set()

        for key3PC, pp in itertools.chain(
            self.sentPrePrepares.items(),
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

        self._logger.trace("{} found {} 3-phase keys to clean".
                           format(self, len(tpcKeys)))
        self._logger.trace("{} found {} request keys to clean".
                           format(self, len(reqKeys)))

        to_clean_up = (
            self.pre_prepare_tss,
            self.sentPrePrepares,
            self.prePrepares,
            self.prepares,
            self.commits,
            self.batches,
            self.requested_pre_prepares,
            self.requested_prepares,
            self.requested_commits,
            self.pre_prepares_stashed_for_incorrect_time
        )
        for request_key in tpcKeys:
            for coll in to_clean_up:
                coll.pop(request_key, None)

        for request_key in reqKeys:
            self._requests.free(request_key)
            for ledger_id, keys in self.requestQueues.items():
                if request_key in keys:
                    self.l_discard_req_key(ledger_id, request_key)
            self._logger.trace('{} freed request {} from previous checkpoints'
                               .format(self, request_key))

        # ToDo: do we need ordered messages there?
        self.ordered.clear_below_view(self.view_no - 1)

        # BLS multi-sig:
        self.l_bls_bft_replica.gc(till3PCKey)

    """Method from legacy code"""
    def l_discard_req_key(self, ledger_id, req_key):
        self.requestQueues[ledger_id].discard(req_key)

    """Method from legacy code"""
    def l_remove_stashed_checkpoints(self, view_no=None):
        """
        Remove stashed received checkpoints up to `till_3pc_key` if provided,
        otherwise remove all stashed received checkpoints
        """
        # ToDo: Maybe need to change message format?
        self._bus.send(RemoveStashedCheckpoints(view_no=view_no,
                                                all=True,
                                                start_no=0,
                                                end_no=0))
        # ToDo: Should we send some notification for checkpoint service?
        # if till_3pc_key is None:
        #     self.stashedRecvdCheckpoints.clear()
        #     self._logger.info('{} removing all stashed checkpoints'.format(self))
        #     return
        #
        # for view_no in list(self.stashedRecvdCheckpoints.keys()):
        #
        #     if view_no < till_3pc_key[0]:
        #         self._logger.info('{} removing stashed checkpoints for view {}'.format(self, view_no))
        #         del self.stashedRecvdCheckpoints[view_no]
        #
        #     elif view_no == till_3pc_key[0]:
        #         for (s, e) in list(self.stashedRecvdCheckpoints[view_no].keys()):
        #             if e <= till_3pc_key[1]:
        #                 self._logger.info('{} removing stashed checkpoints: '
        #                                  'viewNo={}, seqNoStart={}, seqNoEnd={}'.
        #                                  format(self, view_no, s, e))
        #                 del self.stashedRecvdCheckpoints[view_no][(s, e)]
        #         if len(self.stashedRecvdCheckpoints[view_no]) == 0:
        #             del self.stashedRecvdCheckpoints[view_no]

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
        self._bus.send(ex)

    def _validate(self, msg):
        return self._validator.validate(msg)

    """Method from legacy code"""
    def l_compact_primary_names(self):
        min_allowed_view_no = self.view_no - 1
        views_to_remove = []
        for view_no in self.primary_names:
            if view_no >= min_allowed_view_no:
                break
            views_to_remove.append(view_no)
        for view_no in views_to_remove:
            self.primary_names.pop(view_no)

    """Method from legacy code"""
    def l_can_process_pre_prepare(self, pre_prepare: PrePrepare, sender: str):
        """
        Decide whether this replica is eligible to process a PRE-PREPARE.

        :param pre_prepare: a PRE-PREPARE msg to process
        :param sender: the name of the node that sent the PRE-PREPARE msg
        """
        # TODO: Check whether it is rejecting PRE-PREPARE from previous view

        # PRE-PREPARE should not be sent from non primary
        if not self.l_isMsgFromPrimary(pre_prepare, sender):
            return PP_CHECK_NOT_FROM_PRIMARY

        # Already has a PRE-PREPARE with same 3 phase key
        if (pre_prepare.viewNo, pre_prepare.ppSeqNo) in self.legacy_preprepares:
            return PP_CHECK_DUPLICATE

        if not self.l_is_pre_prepare_time_acceptable(pre_prepare, sender):
            return PP_CHECK_WRONG_TIME

        if compare_3PC_keys((pre_prepare.viewNo, pre_prepare.ppSeqNo),
                            self.__last_pp_3pc) > 0:
            return PP_CHECK_OLD  # ignore old pre-prepare

        if self.l_nonFinalisedReqs(pre_prepare.reqIdr):
            return PP_CHECK_REQUEST_NOT_FINALIZED

        if not self.l__is_next_pre_prepare(pre_prepare.viewNo,
                                           pre_prepare.ppSeqNo):
            return PP_CHECK_NOT_NEXT

        if f.POOL_STATE_ROOT_HASH.nm in pre_prepare and \
                pre_prepare.poolStateRootHash != self.l_stateRootHash(POOL_LEDGER_ID):
            return PP_CHECK_INCORRECT_POOL_STATE_ROOT

        # BLS multi-sig:
        status = self.l_bls_bft_replica.validate_pre_prepare(pre_prepare,
                                                             sender)
        if status is not None:
            return status
        return None

    def _schedule(self, func, delay):
        self._timer.schedule(delay, func)

    """Method from legacy code"""
    def l_process_valid_preprepare(self, pre_prepare: PrePrepare, sender: str):
        self.first_batch_after_catchup = False
        old_state_root = self.l_stateRootHash(pre_prepare.ledgerId, to_str=False)
        old_txn_root = self.l_txnRootHash(pre_prepare.ledgerId)
        if self.is_master:
            self._logger.debug('{} state root before processing {} is {}, {}'.format(
                self,
                pre_prepare,
                old_state_root,
                old_txn_root))

        # 1. APPLY
        reqs, invalid_indices, rejects, suspicious = self.l_apply_pre_prepare(pre_prepare)

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
            why_not_applied = self.l_validate_applied_pre_prepare(pre_prepare,
                                                                  reqs, invalid_indices, invalid_from_pp)

        # 4. IF NOT VALID AFTER APPLYING - REVERT
        if why_not_applied is not None:
            if self.is_master:
                self.l_revert(pre_prepare.ledgerId,
                              old_state_root,
                              len(pre_prepare.reqIdr) - len(invalid_indices))
            return why_not_applied

        # 5. EXECUTE HOOK
        if self.is_master:
            try:
                self.l_execute_hook(ReplicaHooks.APPLY_PPR, pre_prepare)
            except Exception as ex:
                self._logger.warning('{} encountered exception in replica '
                                     'hook {} : {}'.
                                     format(self, ReplicaHooks.APPLY_PPR, ex))
                self.l_revert(pre_prepare.ledgerId,
                              old_state_root,
                              len(pre_prepare.reqIdr) - len(invalid_from_pp))
                return PP_APPLY_HOOK_ERROR

        # 6. TRACK APPLIED
        self.send_outbox(rejects)
        self.l_addToPrePrepares(pre_prepare)

        if self.is_master:
            # BLS multi-sig:
            self.l_bls_bft_replica.process_pre_prepare(pre_prepare, sender)
            self._logger.trace("{} saved shared multi signature for "
                               "root".format(self, old_state_root))

        if not self.is_master:
            self.db_manager.get_store(LAST_SENT_PP_STORE_LABEL).store_last_sent_pp_seq_no(
                self._data.inst_id, pre_prepare.ppSeqNo)
        self.l_trackBatches(pre_prepare, old_state_root)
        key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
        self._logger.debug("{} processed incoming PRE-PREPARE{}".format(self, key),
                           extra={"tags": ["processing"]})
        return None

    """Method from legacy code"""
    def l_enqueue_pre_prepare(self, pre_prepare: PrePrepare, sender: str,
                              nonFinReqs: Set = None):
        if nonFinReqs:
            self._logger.info("{} - Queueing pre-prepares due to unavailability of finalised "
                              "requests. PrePrepare {} from {}".format(self, pre_prepare, sender))
            self.prePreparesPendingFinReqs.append((pre_prepare, sender, nonFinReqs))
        else:
            # Possible exploit, an malicious party can send an invalid
            # pre-prepare and over-write the correct one?
            self._logger.info("Queueing pre-prepares due to unavailability of previous pre-prepares. {} from {}".
                              format(pre_prepare, sender))
            self.prePreparesPendingPrevPP[pre_prepare.viewNo, pre_prepare.ppSeqNo] = (pre_prepare, sender)

    """Method from legacy code"""
    def l_request_propagates_if_needed(self, bad_reqs: list, pre_prepare: PrePrepare):
        if any(pre_prepare is pended[0] for pended in self.prePreparesPendingFinReqs):
            self._bus.send(RequestPropagates(bad_reqs))

    """Method from legacy code"""
    def l_request_missing_three_phase_messages(self, view_no: int, seq_frm: int, seq_to: int) -> None:
        for pp_seq_no in range(seq_frm, seq_to + 1):
            key = (view_no, pp_seq_no)
            self._request_pre_prepare(key)
            self._request_prepare(key)
            self._request_commit(key)

    def _request_three_phase_msg(self, three_pc_key: Tuple[int, int],
                                 stash: Dict[Tuple[int, int], Optional[Tuple[str, str, str]]],
                                 msg_type: str,
                                 recipients: Optional[List[str]] = None,
                                 stash_data: Optional[Tuple[str, str, str]] = None) -> bool:
        if three_pc_key in stash:
            self._logger.debug('{} not requesting {} since already '
                               'requested for {}'.format(self, msg_type, three_pc_key))
            return False

        # TODO: Using a timer to retry would be a better thing to do
        self._logger.trace('{} requesting {} for {} from {}'.format(
            self, msg_type, three_pc_key, recipients))
        # An optimisation can be to request PRE-PREPARE from f+1 or
        # f+x (f+x<2f) nodes only rather than 2f since only 1 correct
        # PRE-PREPARE is needed.
        self._request_msg(msg_type, {f.INST_ID.nm: self._data.inst_id,
                                     f.VIEW_NO.nm: three_pc_key[0],
                                     f.PP_SEQ_NO.nm: three_pc_key[1]},
                          frm=recipients)

        stash[three_pc_key] = stash_data
        return True

    def _request_pre_prepare(self, three_pc_key: Tuple[int, int],
                             stash_data: Optional[Tuple[str, str, str]] = None) -> bool:
        """
        Request preprepare
        """
        recipients = self.primary_name
        return self._request_three_phase_msg(three_pc_key,
                                             self.requested_pre_prepares,
                                             PREPREPARE,
                                             recipients,
                                             stash_data)

    def _request_prepare(self, three_pc_key: Tuple[int, int],
                         recipients: List[str] = None,
                         stash_data: Optional[Tuple[str, str, str]] = None) -> bool:
        """
        Request preprepare
        """
        if recipients is None:
            recipients = self._network.connecteds.copy()
            primary_name = self.primary_name[:self.primary_name.rfind(":")]
            if primary_name in recipients:
                del recipients[primary_name]
        return self._request_three_phase_msg(three_pc_key, self.requested_prepares, PREPARE, recipients, stash_data)

    def _request_commit(self, three_pc_key: Tuple[int, int],
                        recipients: List[str] = None) -> bool:
        """
        Request commit
        """
        return self._request_three_phase_msg(three_pc_key, self.requested_commits, COMMIT, recipients)

    def _request_msg(self, typ, params: Dict, frm: List[str] = None):
        self._network.send(MessageReq(**{
            f.MSG_TYPE.nm: typ,
            f.PARAMS.nm: params
        }), dst=frm)

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
                self._logger.info('{} Setting last ordered for non-master as {}'.
                                  format(self, self.last_ordered_3pc))
                self.last_ordered_3pc = (self.view_no, lowest_prepared - 1)
                self.l_update_watermark_from_3pc()
                self.first_batch_after_catchup = False

    """Method from legacy code"""
    def l_stateRootHash(self, ledger_id, to_str=True, committed=False):
        if not self.is_master:
            return None
        state = self.db_manager.get_state(ledger_id)
        root = state.committedHeadHash if committed else state.headHash
        if to_str:
            root = self._state_root_serializer.serialize(bytes(root))
        return root

    """Method from legacy code"""
    def l_isMsgFromPrimary(self, msg, sender: str) -> bool:
        """
        Return whether this message was from primary replica
        :param msg:
        :param sender:
        :return:
        """
        if self.l_isMsgForCurrentView(msg):
            return self.primary_name == sender
        try:
            return self.primary_names[msg.viewNo] == sender
        except KeyError:
            return False

    """Method from legacy code"""
    def l_isMsgForCurrentView(self, msg):
        """
        Return whether this request's view number is equal to the current view
        number of this replica.
        """
        viewNo = getattr(msg, "viewNo", None)
        return viewNo == self.view_no

    """Method from legacy code"""
    def l_is_pre_prepare_time_correct(self, pp: PrePrepare, sender: str) -> bool:
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
                ((pp, sender) not in self.pre_prepare_tss[tpcKey])):
            return False
        else:
            return (
                abs(pp.ppTime - self.pre_prepare_tss[tpcKey][pp, sender]) <=
                self._config.ACCEPTABLE_DEVIATION_PREPREPARE_SECS
            )

    """Method from legacy code"""
    def l_is_pre_prepare_time_acceptable(self, pp: PrePrepare, sender: str) -> bool:
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
        correct = self.l_is_pre_prepare_time_correct(pp, sender)
        if not correct:
            if key in self.pre_prepares_stashed_for_incorrect_time and \
                    self.pre_prepares_stashed_for_incorrect_time[key][-1]:
                self._logger.debug('{} marking time as correct for {}'.format(self, pp))
                correct = True
            else:
                self._logger.warning('{} found {} to have incorrect time.'.format(self, pp))
        return correct

    """Method from legacy code"""
    def l_nonFinalisedReqs(self, reqKeys: List[Tuple[str, int]]):
        """
        Check if there are any requests which are not finalised, i.e for
        which there are not enough PROPAGATEs
        """
        return {key for key in reqKeys if not self._requests.is_finalised(key)}

    """Method from legacy code"""
    def l__is_next_pre_prepare(self, view_no: int, pp_seq_no: int):
        if view_no == self.view_no and pp_seq_no == 1:
            # First PRE-PREPARE in a new view
            return True
        (last_pp_view_no, last_pp_seq_no) = self.__last_pp_3pc
        if last_pp_view_no > view_no:
            return False
        if last_pp_view_no < view_no:
            if view_no != self.view_no:
                return False
            last_pp_seq_no = 0
        if pp_seq_no - last_pp_seq_no > 1:
            return False
        return True

    """Method from legacy code"""
    def l_txnRootHash(self, ledger_str, to_str=True):
        if not self.is_master:
            return None
        ledger = self.db_manager.get_ledger(ledger_str)
        root = ledger.uncommitted_root_hash
        if to_str:
            root = ledger.hashToStr(root)
        return root

    """Method from legacy code"""
    def l_apply_pre_prepare(self, pre_prepare: PrePrepare):
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
            req = self._requests[req_key].finalised
            try:
                self.l_processReqDuringBatch(req,
                                             pre_prepare.ppTime)
            except (InvalidClientMessageException, UnknownIdentifier, SuspiciousPrePrepare) as ex:
                self._logger.warning('{} encountered exception {} while processing {}, '
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
            three_pc_batch = ThreePcBatch.from_pre_prepare(pre_prepare,
                                                           state_root=self.l_stateRootHash(pre_prepare.ledgerId,
                                                                                           to_str=False),
                                                           txn_root=self.l_txnRootHash(pre_prepare.ledgerId,
                                                                                       to_str=False),
                                                           primaries=[],
                                                           valid_digests=self.l_get_valid_req_ids_from_all_requests(
                                                               reqs, invalid_indices))
            self.post_batch_creation(three_pc_batch)

        return reqs, invalid_indices, rejects, suspicious

    """Method from legacy code"""
    def l_get_valid_req_ids_from_all_requests(self, reqs, invalid_indices):
        return [req.key for idx, req in enumerate(reqs) if idx not in invalid_indices]

    """Method from legacy code"""
    def l_validate_applied_pre_prepare(self, pre_prepare: PrePrepare,
                                       reqs, invalid_indices, invalid_from_pp) -> Optional[int]:
        if len(invalid_indices) != len(invalid_from_pp):
            return PP_APPLY_REJECT_WRONG

        digest = Replica.batchDigest(reqs)
        if digest != pre_prepare.digest:
            return PP_APPLY_WRONG_DIGEST

        if self.is_master:
            if pre_prepare.stateRootHash != self.l_stateRootHash(pre_prepare.ledgerId):
                return PP_APPLY_WRONG_STATE

            if pre_prepare.txnRootHash != self.l_txnRootHash(pre_prepare.ledgerId):
                return PP_APPLY_ROOT_HASH_MISMATCH

            # TODO: move this kind of validation to batch handlers
            if f.AUDIT_TXN_ROOT_HASH.nm in pre_prepare and pre_prepare.auditTxnRootHash != self.l_txnRootHash(AUDIT_LEDGER_ID):
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

    """Method from legacy code"""
    def l_revert(self, ledgerId, stateRootHash, reqCount):
        # A batch should only be reverted if all batches that came after it
        # have been reverted
        ledger = self.db_manager.get_ledger(ledgerId)
        state = self.db_manager.get_state(ledgerId)
        self._logger.info('{} reverting {} txns and state root from {} to {} for ledger {}'
                          .format(self, reqCount, Ledger.hashToStr(state.headHash),
                                  Ledger.hashToStr(stateRootHash), ledgerId))
        state.revertToHead(stateRootHash)
        ledger.discardTxns(reqCount)
        self.post_batch_rejection(ledgerId)

    """Method from legacy code"""
    def l_execute_hook(self, hook_id, *args):
        # ToDo: need to receive results from hooks
        self._bus.send(HookMessage(hook=hook_id,
                                   args=args))

    """Method from legacy code"""
    def l_trackBatches(self, pp: PrePrepare, prevStateRootHash):
        # pp.discarded indicates the index from where the discarded requests
        #  starts hence the count of accepted requests, prevStateRoot is
        # tracked to revert this PRE-PREPARE
        self._logger.trace('{} tracking batch for {} with state root {}'.format(
            self, pp, prevStateRootHash))
        # ToDo: for first stage we will exclude metrics
        # if self.is_master:
        #     self._metrics.add_event(MetricsName.THREE_PC_BATCH_SIZE, len(pp.reqIdr))
        # else:
        #     self._metrics.add_event(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, len(pp.reqIdr))

        self.l_batches[(pp.viewNo, pp.ppSeqNo)] = [pp.ledgerId, pp.discarded,
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
            self._logger.debug(
                '{} cannot set lastPrePrepareSeqNo to {} as its '
                'already {}'.format(
                    self, n, self._lastPrePrepareSeqNo))

    """Method from legacy code"""
    def l_addToPrePrepares(self, pp: PrePrepare) -> None:
        """
        Add the specified PRE-PREPARE to this replica's list of received
        PRE-PREPAREs and try sending PREPARE

        :param pp: the PRE-PREPARE to add to the list
        """
        key = (pp.viewNo, pp.ppSeqNo)
        # ToDo:
        self.prePrepares[key] = pp
        self.lastPrePrepareSeqNo = pp.ppSeqNo
        self.last_accepted_pre_prepare_time = pp.ppTime
        self.l_dequeue_prepares(*key)
        self.l_dequeue_commits(*key)
        self.stats.inc(TPCStat.PrePrepareRcvd)
        self.l_tryPrepare(pp)

    """Method from legacy code"""
    def l_dequeue_prepares(self, viewNo: int, ppSeqNo: int):
        key = (viewNo, ppSeqNo)
        if key in self.preparesWaitingForPrePrepare:
            i = 0
            # Keys of pending prepares that will be processed below
            while self.preparesWaitingForPrePrepare[key]:
                prepare, sender = self.preparesWaitingForPrePrepare[
                    key].popleft()
                self._logger.debug("{} popping stashed PREPARE{}".format(self, key))
                self.process_prepare(prepare, sender)
                i += 1
            self.preparesWaitingForPrePrepare.pop(key)
            self._logger.debug("{} processed {} PREPAREs waiting for PRE-PREPARE for"
                               " view no {} and seq no {}".format(self, i, viewNo, ppSeqNo))

    """Method from legacy code"""
    def l_dequeue_commits(self, viewNo: int, ppSeqNo: int):
        key = (viewNo, ppSeqNo)
        if key in self.commitsWaitingForPrepare:
            if not self.l_has_prepared(key):
                self._logger.debug('{} has not pre-prepared {}, will dequeue the '
                                   'COMMITs later'.format(self, key))
                return
            i = 0
            # Keys of pending prepares that will be processed below
            while self.commitsWaitingForPrepare[key]:
                commit, sender = self.commitsWaitingForPrepare[
                    key].popleft()
                self._logger.debug("{} popping stashed COMMIT{}".format(self, key))
                self.process_commit(commit, sender)

                i += 1
            self.commitsWaitingForPrepare.pop(key)
            self._logger.debug("{} processed {} COMMITs waiting for PREPARE for"
                               " view no {} and seq no {}".format(self, i, viewNo, ppSeqNo))

    """Method from legacy code"""
    def l_tryPrepare(self, pp: PrePrepare):
        """
        Try to send the Prepare message if the PrePrepare message is ready to
        be passed into the Prepare phase.
        """
        rv, msg = self.l_canPrepare(pp)
        if rv:
            self.l_doPrepare(pp)
        else:
            self._logger.debug("{} cannot send PREPARE since {}".format(self, msg))

    """Method from legacy code"""
    def l_canPrepare(self, ppReq) -> (bool, str):
        """
        Return whether the batch of requests in the PRE-PREPARE can
        proceed to the PREPARE step.

        :param ppReq: any object with identifier and requestId attributes
        """
        if self.l_has_sent_prepare(ppReq):
            return False, 'has already sent PREPARE for {}'.format(ppReq)
        return True, ''

    """Method from legacy code"""
    def l_has_sent_prepare(self, request) -> bool:
        return self.prepares.hasPrepareFrom(request, self.name)

    """Method from legacy code"""
    def l_doPrepare(self, pp: PrePrepare):
        self._logger.debug("{} Sending PREPARE{} at {}".format(
            self, (pp.viewNo, pp.ppSeqNo), self.get_current_time()))
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
        if self.is_master:
            rv = self.l_execute_hook(ReplicaHooks.CREATE_PR, prepare, pp)
            prepare = rv if rv is not None else prepare
        self._network.send(prepare, TPCStat.PrepareSent)
        self.l_addToPrepares(prepare, self.name)

    """Method from legacy code"""
    def l_update_watermark_from_3pc(self):
        if (self.last_ordered_3pc is not None) and (self.last_ordered_3pc[0] == self.view_no):
            self._logger.info("update_watermark_from_3pc to {}".format(self.last_ordered_3pc))
            self.h = self.last_ordered_3pc[1]
        else:
            self._logger.info("try to update_watermark_from_3pc but last_ordered_3pc is None")

    """Method from legacy code"""
    def l_has_prepared(self, key):
        if not self.l_getPrePrepare(*key):
            return False
        if ((key not in self.prepares and key not in self.sentPrePrepares) and
                (key not in self.preparesWaitingForPrePrepare)):
            return False
        return True

    """Method from legacy code"""
    def l_getPrePrepare(self, viewNo, ppSeqNo):
        key = (viewNo, ppSeqNo)
        if key in self.sentPrePrepares:
            return self.sentPrePrepares[key]
        if key in self.prePrepares:
            return self.prePrepares[key]
        return None

    """Method from legacy code"""
    def l_addToPrepares(self, prepare: Prepare, sender: str):
        """
        Add the specified PREPARE to this replica's list of received
        PREPAREs and try sending COMMIT

        :param prepare: the PREPARE to add to the list
        """
        # BLS multi-sig:
        self.l_bls_bft_replica.process_prepare(prepare, sender)

        self.prepares.addVote(prepare, sender)
        self.l_dequeue_commits(prepare.viewNo, prepare.ppSeqNo)
        self.l_tryCommit(prepare)

    """Method from legacy code"""
    def l_tryCommit(self, prepare: Prepare):
        """
        Try to commit if the Prepare message is ready to be passed into the
        commit phase.
        """
        rv, reason = self.l_canCommit(prepare)
        if rv:
            pp = self.l_getPrePrepare(prepare.viewNo, prepare.ppSeqNo)
            self._consensus_data_helper.prepare_batch(pp)
            self.l_doCommit(prepare)
        else:
            self._logger.debug("{} cannot send COMMIT since {}".format(self, reason))

    def l_doCommit(self, p: Prepare):
        """
        Create a commit message from the given Prepare message and trigger the
        commit phase
        :param p: the prepare message
        """
        key_3pc = (p.viewNo, p.ppSeqNo)
        self._logger.debug("{} Sending COMMIT{} at {}".format(self, key_3pc, self.get_current_time()))

        params = [
            self._data.inst_id, p.viewNo, p.ppSeqNo
        ]

        pre_prepare = self.l_getPrePrepare(*key_3pc)

        # BLS multi-sig:
        if p.stateRootHash is not None:
            pre_prepare = self.l_getPrePrepare(*key_3pc)
            params = self.l_bls_bft_replica.update_commit(params, pre_prepare)

        commit = Commit(*params)

        self._network.send(commit, TPCStat.CommitSent)
        self.l_addToCommits(commit, self.name)

    """Method from legacy code"""
    def l_addToCommits(self, commit: Commit, sender: str):
        """
        Add the specified COMMIT to this replica's list of received
        commit requests.

        :param commit: the COMMIT to add to the list
        :param sender: the name of the node that sent the COMMIT
        """
        # BLS multi-sig:
        self.l_bls_bft_replica.process_commit(commit, sender)

        self.commits.addVote(commit, sender)
        self.l_tryOrder(commit)

    """Method from legacy code"""
    def l_tryOrder(self, commit: Commit):
        """
        Try to order if the Commit message is ready to be ordered.
        """
        canOrder, reason = self.l_canOrder(commit)
        if canOrder:
            self._logger.trace("{} returning request to node".format(self))
            self.l_doOrder(commit)
        else:
            self._logger.debug("{} cannot return request to node: {}".format(self, reason))
        return canOrder

    """Method from legacy code"""
    def l_doOrder(self, commit: Commit):
        key = (commit.viewNo, commit.ppSeqNo)
        self._logger.debug("{} ordering COMMIT {}".format(self, key))
        return self.l_order_3pc_key(key)

    """Method from legacy code"""
    def l_order_3pc_key(self, key):
        pp = self.l_getPrePrepare(*key)
        if pp is None:
            raise ValueError(
                "{} no PrePrepare with a 'key' {} found".format(self, key)
            )

        self._freshness_checker.update_freshness(ledger_id=pp.ledgerId,
                                                 ts=pp.ppTime)

        self.l_addToOrdered(*key)
        invalid_indices = invalid_index_serializer.deserialize(pp.discarded)
        invalid_reqIdr = []
        valid_reqIdr = []
        for ind, reqIdr in enumerate(pp.reqIdr):
            if ind in invalid_indices:
                invalid_reqIdr.append(reqIdr)
            else:
                valid_reqIdr.append(reqIdr)
            self._requests.ordered_by_replica(reqIdr)

        ordered = Ordered(self._data.inst_id,
                          pp.viewNo,
                          valid_reqIdr,
                          invalid_reqIdr,
                          pp.ppSeqNo,
                          pp.ppTime,
                          pp.ledgerId,
                          pp.stateRootHash,
                          pp.txnRootHash,
                          pp.auditTxnRootHash if f.AUDIT_TXN_ROOT_HASH.nm in pp else None,
                          self._get_primaries_for_ordered(pp))
        if self.is_master:
            rv = self.l_execute_hook(ReplicaHooks.CREATE_ORD, ordered, pp)
            ordered = rv if rv is not None else ordered

        self.l_discard_ordered_req_keys(pp)

        self._bus.send(ordered, TPCStat.OrderSent)

        ordered_msg = "{} ordered batch request, view no {}, ppSeqNo {}, ledger {}, " \
                      "state root {}, txn root {}, audit root {}".format(self, pp.viewNo, pp.ppSeqNo, pp.ledgerId,
                                                                         pp.stateRootHash, pp.txnRootHash,
                                                                         pp.auditTxnRootHash)
        self._logger.debug("{}, requests ordered {}, discarded {}".
                           format(ordered_msg, valid_reqIdr, invalid_reqIdr))
        self._logger.info("{}, requests ordered {}, discarded {}".
                          format(ordered_msg, len(valid_reqIdr), len(invalid_reqIdr)))

        # ToDo: add metrics in integration phase
        # if self.isMaster:
        #     self.metrics.add_event(MetricsName.ORDERED_BATCH_SIZE, len(valid_reqIdr) + len(invalid_reqIdr))
        #     self.metrics.add_event(MetricsName.ORDERED_BATCH_INVALID_COUNT, len(invalid_reqIdr))
        # else:
        #     self.metrics.add_event(MetricsName.BACKUP_ORDERED_BATCH_SIZE, len(valid_reqIdr))
        # TODO: remove it after INDY-2137
        self.l_addToCheckpoint(pp.ppSeqNo, pp.digest, pp.ledgerId, pp.viewNo)

        # BLS multi-sig:
        self.l_bls_bft_replica.process_order(key, self._data.quorums, pp)

        return True

    """Method from legacy code"""
    # TODO: remove it after INDY-2137
    def l_addToCheckpoint(self, ppSeqNo, digest, ledger_id, view_no):
        for (s, e) in self._data.checkpoints.keys():
            if s <= ppSeqNo <= e:
                state = self._data.checkpoints[s, e]  # type: CheckpointState
                state.digests.append(digest)
                state = updateNamedTuple(state, seqNo=ppSeqNo)
                self._data.checkpoints[s, e] = state
                break
        else:
            s, e = ppSeqNo, math.ceil(ppSeqNo / self._config.CHK_FREQ) * self._config.CHK_FREQ
            self._logger.debug("{} adding new checkpoint state for {}".format(self, (s, e)))
            state = CheckpointState(ppSeqNo, [digest, ], None, {}, False)
            self._data.checkpoints[s, e] = state

        if state.seqNo == e:
            if len(state.digests) == self._config.CHK_FREQ:
                self.do_checkpoint(state, s, e, ledger_id, view_no)
            self.process_stashed_checkpoints(start_no=s,
                                             end_no=e,
                                             view_no=view_no)

    """Method from legacy code"""
    def process_stashed_checkpoints(self, start_no, end_no, view_no):

        self._bus.send(RemoveStashedCheckpoints(start_no=start_no,
                                                end_no=end_no,
                                                view_no=view_no,
                                                all=False))
        # ToDo: remove the next code after integration phase
        # # Remove all checkpoints from previous views if any
        # self._remove_stashed_checkpoints(till_3pc_key=(self.viewNo, 0))
        #
        # if key not in self.stashedRecvdCheckpoints.get(view_no, {}):
        #     self.logger.trace("{} have no stashed checkpoints for {}")
        #     return
        #
        # # Get a snapshot of all the senders of stashed checkpoints for `key`
        # senders = list(self.stashedRecvdCheckpoints[view_no][key].keys())
        # total_processed = 0
        # consumed = 0
        #
        # for sender in senders:
        #     # Check if the checkpoint from `sender` is still in
        #     # `stashedRecvdCheckpoints` because it might be removed from there
        #     # in case own checkpoint was stabilized when we were processing
        #     # stashed checkpoints from previous senders in this loop
        #     if view_no in self.stashedRecvdCheckpoints \
        #             and key in self.stashedRecvdCheckpoints[view_no] \
        #             and sender in self.stashedRecvdCheckpoints[view_no][key]:
        #         if self.process_checkpoint(
        #                 self.stashedRecvdCheckpoints[view_no][key].pop(sender),
        #                 sender):
        #             consumed += 1
        #         # Note that if `process_checkpoint` returned False then the
        #         # checkpoint from `sender` was re-stashed back to
        #         # `stashedRecvdCheckpoints`
        #         total_processed += 1
        #
        # # If we have consumed stashed checkpoints for `key` from all the
        # # senders then remove entries which have become empty
        # if view_no in self.stashedRecvdCheckpoints \
        #         and key in self.stashedRecvdCheckpoints[view_no] \
        #         and len(self.stashedRecvdCheckpoints[view_no][key]) == 0:
        #     del self.stashedRecvdCheckpoints[view_no][key]
        #     if len(self.stashedRecvdCheckpoints[view_no]) == 0:
        #         del self.stashedRecvdCheckpoints[view_no]
        #
        # restashed = total_processed - consumed
        # self.logger.info('{} processed {} stashed checkpoints for {}, '
        #                  '{} of them were stashed again'.
        #                  format(self, total_processed, key, restashed))
        #
        # return total_processed

    def do_checkpoint(self, state: CheckpointState,
                      start_no: int,
                      end_no: int,
                      ledger_id: int,
                      view_no: int):
        self._bus.send(DoCheckpointMessage(state,
                                           start_no,
                                           end_no,
                                           ledger_id,
                                           view_no))

    """Method from legacy code"""
    def l_addToOrdered(self, view_no: int, pp_seq_no: int):
        self.ordered.add(view_no, pp_seq_no)
        self.last_ordered_3pc = (view_no, pp_seq_no)

        self.requested_pre_prepares.pop((view_no, pp_seq_no), None)
        self.requested_prepares.pop((view_no, pp_seq_no), None)
        self.requested_commits.pop((view_no, pp_seq_no), None)

    def _get_primaries_for_ordered(self, pp):
        ledger = self.db_manager.get_ledger(AUDIT_LEDGER_ID)
        for index, txn in enumerate(ledger.get_uncommitted_txns()):
            payload_data = get_payload_data(txn)
            if pp.ppSeqNo == payload_data[AUDIT_TXN_PP_SEQ_NO] and \
                    pp.viewNo == payload_data[AUDIT_TXN_VIEW_NO]:
                txn_primaries = payload_data[AUDIT_TXN_PRIMARIES]
                if isinstance(txn_primaries, Iterable):
                    return txn_primaries
                elif isinstance(txn_primaries, int):
                    last_primaries_seq_no = get_seq_no(txn) - txn_primaries
                    return get_payload_data(
                        ledger.get_by_seq_no_uncommitted(last_primaries_seq_no))[AUDIT_TXN_PRIMARIES]
                break
        else:
            return self.db_manager.primaries

    """Method from legacy code"""
    def l_discard_ordered_req_keys(self, pp: PrePrepare):
        for k in pp.reqIdr:
            # Using discard since the key may not be present as in case of
            # primary, the key was popped out while creating PRE-PREPARE.
            # Or in case of node catching up, it will not validate
            # PRE-PREPAREs or PREPAREs but will only validate number of COMMITs
            #  and their consistency with PRE-PREPARE of PREPAREs
            self.l_discard_req_key(pp.ledgerId, k)

    """Method from legacy code"""
    def l_canOrder(self, commit: Commit) -> Tuple[bool, Optional[str]]:
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

        if commit.ppSeqNo > 1 and not self.l_all_prev_ordered(commit):
            viewNo, ppSeqNo = commit.viewNo, commit.ppSeqNo
            if viewNo not in self.stashed_out_of_order_commits:
                self.stashed_out_of_order_commits[viewNo] = {}
            self.stashed_out_of_order_commits[viewNo][ppSeqNo] = commit
            self._out_of_order_repeater.start()
            return False, "stashing {} since out of order". \
                format(commit)

        return True, None

    """Method from legacy code"""
    def l_process_stashed_out_of_order_commits(self):
        # This method is called periodically to check for any commits that
        # were stashed due to lack of commits before them and orders them if it
        # can

        if not self.can_order():
            return

        self._logger.debug('{} trying to order from out of order commits. '
                           'Len(stashed_out_of_order_commits) == {}'
                           .format(self, len(self.stashed_out_of_order_commits)))
        if self.last_ordered_3pc:
            lastOrdered = self.last_ordered_3pc
            vToRemove = set()
            for v in self.stashed_out_of_order_commits:
                if v < lastOrdered[0]:
                    self._logger.debug(
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
                            (v > lastOrdered[0] and self.l_isLowestCommitInView(commit)):
                        self._logger.debug("{} ordering stashed commit {}".format(self, commit))
                        if self.l_tryOrder(commit):
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
            self._logger.debug('{} last_ordered_3pc if False. '
                               'Len(stashed_out_of_order_commits) == {}'
                               .format(self, len(self.stashed_out_of_order_commits)))

    def l_isLowestCommitInView(self, commit):
        view_no = commit.viewNo
        if view_no > self.view_no:
            self._logger.debug('{} encountered {} which belongs to a later view'.format(self, commit))
            return False
        return commit.ppSeqNo == 1

    """Method from legacy code"""
    def l_all_prev_ordered(self, commit: Commit):
        """
        Return True if all previous COMMITs have been ordered
        """
        # TODO: This method does a lot of work, choose correct data
        # structures to make it efficient.

        viewNo, ppSeqNo = commit.viewNo, commit.ppSeqNo

        if self.last_ordered_3pc == (viewNo, ppSeqNo - 1):
            # Last ordered was in same view as this COMMIT
            return True

        # if some PREPAREs/COMMITs were completely missed in the same view
        toCheck = set()
        toCheck.update(set(self.sentPrePrepares.keys()))
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

    """Method from legacy code"""
    def l_canCommit(self, prepare: Prepare) -> (bool, str):
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
        if self.l_hasCommitted(prepare):
            return False, 'has already sent COMMIT for {}'.format(prepare)
        return True, ''

    """Method from legacy code"""
    def l_hasCommitted(self, request) -> bool:
        return self.commits.hasCommitFrom(ThreePhaseKey(
            request.viewNo, request.ppSeqNo), self.name)

    def post_batch_creation(self, three_pc_batch: ThreePcBatch):
        """
        A batch of requests has been created and has been applied but
        committed to ledger and state.
        :param ledger_id:
        :param state_root: state root after the batch creation
        :return:
        """
        ledger_id = three_pc_batch.ledger_id
        if ledger_id != POOL_LEDGER_ID and not three_pc_batch.primaries:
            three_pc_batch.primaries = self._write_manager.future_primary_handler.get_last_primaries() or self._data.primaries
        if self._write_manager.is_valid_ledger_id(ledger_id):
            self._write_manager.post_apply_batch(three_pc_batch)
        else:
            self._logger.debug('{} did not know how to handle for ledger {}'.format(self, ledger_id))

    def send_outbox(self, msg):
        self._bus.send(OutboxMessage(msg=msg))

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
            self._logger.debug('{} did not know how to handle for ledger {}'.format(self, ledger_id))

    """Method from legacy code"""
    def l_ledger_id_for_request(self, request: Request):
        if request.operation.get(TXN_TYPE) is None:
            raise ValueError(
                "{} TXN_TYPE is not defined for request {}".format(self, request)
            )

        typ = request.operation[TXN_TYPE]
        return self._write_manager.type_to_ledger_id[typ]

    """Method from legacy code"""
    def l_do_dynamic_validation(self, request: Request, req_pp_time: int):
        """
                State based validation
                """
        # Digest validation
        # TODO implicit caller's context: request is processed by (master) replica
        # as part of PrePrepare 3PC batch
        ledger_id, seq_no = self.db_manager.get_store(SEQ_NO_DB_LABEL).get_by_payload_digest(request.payload_digest)
        if ledger_id is not None and seq_no is not None:
            raise SuspiciousPrePrepare('Trying to order already ordered request')

        ledger = self.db_manager.get_ledger(self.l_ledger_id_for_request(request))
        for txn in ledger.uncommittedTxns:
            if get_payload_digest(txn) == request.payload_digest:
                raise SuspiciousPrePrepare('Trying to order already ordered request')

        # TAA validation
        # For now, we need to call taa_validation not from dynamic_validation because
        # req_pp_time is required
        self._write_manager.do_taa_validation(request, req_pp_time, self._config)
        self._write_manager.dynamic_validation(request)

    """Method from legacy code"""
    def l_processReqDuringBatch(self,
                                req: Request,
                                cons_time: int):
        """
                This method will do dynamic validation and apply requests.
                If there is any errors during validation it would be raised
                """
        if self.is_master:
            self.l_do_dynamic_validation(req, cons_time)
            self._write_manager.apply_request(req, cons_time)

    def can_send_3pc_batch(self):
        if not self._data.is_primary:
            return False
        if not self._data.is_participating:
            return False
        # ToDo: is pre_view_change_in_progress needed?
        # if self.replica.node.pre_view_change_in_progress:
        #     return False
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
                    if self.l_can_log_skip_send_3pc():
                        self._logger.info("{} not creating new batch because there already {} in flight out of {} allowed".
                                          format(self.name, batches_in_flight, self._config.Max3PCBatchesInFlight))
                    return False

        self._skip_send_3pc_ts = None
        return True

    def l_can_log_skip_send_3pc(self):
        current_time = time.perf_counter()
        if self._skip_send_3pc_ts is None:
            self._skip_send_3pc_ts = current_time
            return True

        if current_time - self._skip_send_3pc_ts > self._config.Max3PCBatchWait:
            self._skip_send_3pc_ts = current_time
            return True

        return False

    def can_order(self):
        if self._data.is_participating:
            return True
        if self._data.is_synced and self._data.legacy_vc_in_progress:
            return True
        return False

    @staticmethod
    def generateName(node_name: str, inst_id: int):
        """
        Create and return the name for a replica using its nodeName and
        instanceId.
         Ex: Alpha:1
        """

        if isinstance(node_name, str):
            # Because sometimes it is bytes (why?)
            if ":" in node_name:
                # Because in some cases (for requested messages) it
                # already has ':'. This should be fixed.
                return node_name
        return "{}:{}".format(node_name, inst_id)
