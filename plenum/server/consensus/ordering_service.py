from collections import defaultdict, OrderedDict
from functools import partial
from typing import Tuple, List, Set, Optional

from sortedcontainers import SortedList

from common.serializers.serialization import state_roots_serializer, invalid_index_serializer
from crypto.bls.bls_bft_replica import BlsBftReplica
from plenum.common.config_util import getConfig
from plenum.common.constants import POOL_LEDGER_ID, SEQ_NO_DB_LABEL, ReplicaHooks, AUDIT_LEDGER_ID, TXN_TYPE
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.exceptions import SuspiciousNode, InvalidClientMessageException, SuspiciousPrePrepare, \
    UnknownIdentifier
# from plenum.common.messages.internal_messages import ViewChangeInProgress, LedgerSyncStatus, ParticipatingStatus
from plenum.common.ledger import Ledger
from plenum.common.messages.internal_messages import SuspiciousMessage, HookMessage, OutboxMessage
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit, Reject
from plenum.common.metrics_collector import MetricsName
from plenum.common.request import Request
from plenum.common.stashing_router import StashingRouter
from plenum.common.timer import TimerService
from plenum.common.txn_util import get_payload_digest
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys, updateNamedTuple, SortedDict
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.propagator import Requests
from plenum.server.replica import PP_APPLY_REJECT_WRONG, PP_APPLY_WRONG_DIGEST, PP_APPLY_WRONG_STATE, \
    PP_APPLY_ROOT_HASH_MISMATCH, PP_APPLY_HOOK_ERROR, PP_SUB_SEQ_NO_WRONG, PP_NOT_FINAL, PP_APPLY_AUDIT_HASH_MISMATCH, \
    PP_REQUEST_ALREADY_ORDERED, PP_CHECK_NOT_FROM_PRIMARY, PP_CHECK_TO_PRIMARY, PP_CHECK_DUPLICATE, \
    PP_CHECK_INCORRECT_POOL_STATE_ROOT, PP_CHECK_OLD, PP_CHECK_REQUEST_NOT_FINALIZED, PP_CHECK_NOT_NEXT, \
    PP_CHECK_WRONG_TIME, Replica, TPCStat, Stats
from plenum.server.replica_validator_enums import INCORRECT_INSTANCE, DISCARD, INCORRECT_PP_SEQ_NO, ALREADY_ORDERED, \
    STASH_VIEW, FUTURE_VIEW, OLD_VIEW, PROCESS, GREATER_PREP_CERT, STASH_CATCH_UP, CATCHING_UP, OUTSIDE_WATERMARKS, \
    STASH_WATERMARKS
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from plenum.server.suspicion_codes import Suspicions
from stp_core.common.log import getlogger


class ThreePCMsgValidator():
    def __init__(self, data):
        self._data = data

    @property
    def view_no(self):
        return self._data.view_no

    @property
    def low_watermark(self):
        return self._data.low_watermark

    @property
    def high_watermark(self):
        return self._data.high_watermark

    @property
    def legacy_last_prepared_sertificate(self):
        """
        We assume, that prepared list is an ordered list, and the last element is
        the last quorumed Prepared
        """
        if self._data.prepared:
            last_prepared = self._data.prepared[-1]
            return last_prepared.view_no, last_prepared.pp_seq_no
        return self.last_ordered_3pc

    @property
    def last_ordered_3pc(self):
        return self._data.view_no, self._data.pp_seq_no

    @property
    def is_participating(self):
        return self._data.is_participating

    @property
    def legacy_vc_in_progress(self):
        return self._data.legacy_vc_in_progress

    def has_already_ordered(self, view_no, pp_seq_no):
        return compare_3PC_keys((view_no, pp_seq_no),
                                self.last_ordered_3pc) >= 0

    def validate(self, msg):
        view_no = getattr(msg, f.VIEW_NO.nm, None)
        pp_seq_no = getattr(msg, f.PP_SEQ_NO.nm, None)

        # ToDO: this checks should be performed in previous level (ReplicaService)
        # 1. Check INSTANCE_ID
        # if inst_id is None or inst_id != self.replica.instId:
        #     return DISCARD, INCORRECT_INSTANCE

        # 2. Check pp_seq_no
        if pp_seq_no == 0:
            # should start with 1
            return DISCARD, INCORRECT_PP_SEQ_NO

        # 3. Check already ordered
        if self.has_already_ordered(view_no, pp_seq_no):
            return DISCARD, ALREADY_ORDERED

        # 4. Check viewNo
        if view_no > self.view_no:
            return STASH_VIEW, FUTURE_VIEW
        if view_no < self.view_no - 1:
            return DISCARD, OLD_VIEW
        if view_no == self.view_no - 1:
            if not isinstance(msg, Commit):
                return DISCARD, OLD_VIEW
            if not self.legacy_vc_in_progress:
                return DISCARD, OLD_VIEW
            if self.legacy_last_prepared_sertificate is None:
                return DISCARD, OLD_VIEW
            if compare_3PC_keys((view_no, pp_seq_no), self.legacy_last_prepared_sertificate) < 0:
                return DISCARD, GREATER_PREP_CERT
        if view_no == self.view_no and self.legacy_vc_in_progress:
            return STASH_VIEW, FUTURE_VIEW

        # ToDo: we assume, that only is_participating needs checking orderability
        # If Catchup in View Change finished then process Commit messages
        # if self.is_synced and self.legacy_vc_in_progress:
        #     return PROCESS, None

        # 5. Check if Participating
        if not self.is_participating:
            return STASH_CATCH_UP, CATCHING_UP

        # 6. Check watermarks
        if not (self.low_watermark < pp_seq_no <= self.high_watermark):
            return STASH_WATERMARKS, OUTSIDE_WATERMARKS

        return PROCESS, None


class OrderingService:

    def __init__(self,
                 data: ConsensusSharedData,
                 timer: TimerService,
                 bus: InternalBus,
                 network: ExternalBus,
                 write_manager: WriteRequestManager,
                 bls_bft_replica: BlsBftReplica,
                 is_master=True):
        self._data = data
        self._requests = self._data.requests
        self._timer = timer
        self._bus = bus
        self._network = network
        self._write_manager = write_manager
        self._is_master = is_master

        self._config = getConfig()
        self._logger = getlogger()
        self._stasher = StashingRouter(self._config.ORDERING_SERVICE_STASH_LIMIT)
        self._validator = ThreePCMsgValidator(self._data)

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

        self.stats = Stats(TPCStat)

        self.l_batches = OrderedDict()  # type: OrderedDict[Tuple[int, int]]

        self.l_bls_bft_replica = bls_bft_replica

        self._stasher.subscribe(PrePrepare, self.process_preprepare)
        self._stasher.subscribe(Prepare, self.process_prepare)
        self._stasher.subscribe(Commit, self.process_commit)
        self._stasher.subscribe_to(network)

    def process_prepare(self, prepare: Prepare, sender: str):
        result, reason = self._validate(prepare)
        if result == PROCESS:
            self._do_prepare(prepare, sender)
        return result

    def process_commit(self, commit: Commit, sender: str):
        result, reason = self._validate(commit)
        if result == PROCESS:
            self._do_commit(commit, sender)
        return result

    def process_preprepare(self, pre_prepare: PrePrepare, sender: str):
        """
        Validate and process provided PRE-PREPARE, create and
        broadcast PREPARE for it.

        :param pre_prepare: message
        :param sender: name of the node that sent this message
        """
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

    def _do_prepare(self, prepare: Prepare, sender: str):
        pass

    def _do_commit(self, commit: Commit, sender: str):
        pass

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
    def __last_pp_3pc(self):
        last_pp = self._get_last_pp()
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
        return self._data.primary_name

    def _get_last_pp(self):
        # ToDo. Need to take into account sentPreprepares
        last_pp = self._data.preprepared[-1]
        return last_pp

    def report_suspicious_node(self, ex: SuspiciousNode):
        self._bus.send(ex)

    def _validate(self, msg):
        return self._validator.validate(msg)

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
        pass

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
            self.node.last_sent_pp_store_helper.store_last_sent_pp_seq_no(
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
        pass

    """Method from legacy code"""
    def l_request_missing_three_phase_messages(self, view_no: int, seq_frm: int, seq_to: int) -> None:
        pass

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
            return self.primaryNames[msg.viewNo] == sender
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
        self._data.prepared[key] = pp
        self.lastPrePrepareSeqNo = pp.ppSeqNo
        self.last_accepted_pre_prepare_time = pp.ppTime
        self.l_dequeue_prepares(*key)
        self.l_dequeue_commits(*key)
        self.stats.inc(TPCStat.PrePrepareRcvd)
        self.tryPrepare(pp)

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
    def l_update_watermark_from_3pc(self):
        if (self.last_ordered_3pc is not None) and (self.last_ordered_3pc[0] == self.view_no):
            self._logger.info("update_watermark_from_3pc to {}".format(self.last_ordered_3pc))
            self.h = self.last_ordered_3pc[1]
        else:
            self._logger.info("try to update_watermark_from_3pc but last_ordered_3pc is None")

    """Method from legacy code"""
    def l_has_prepared(self, key):
        pass

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

    def l_ledger_id_for_request(self, request: Request):
        if request.operation.get(TXN_TYPE) is None:
            raise ValueError(
                "{} TXN_TYPE is not defined for request {}".format(self, request)
            )

        typ = request.operation[TXN_TYPE]
        return self._write_manager.type_to_ledger_id[typ]

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

        # specific validation for the request txn type
        operation = request.operation
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
            self._write_manager.apply_request(request, cons_time)
