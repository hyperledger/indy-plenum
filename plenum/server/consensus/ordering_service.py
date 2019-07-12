from functools import partial

from crypto.bls.bls_bft_replica import BlsBftReplica
from plenum.common.config_util import getConfig
from plenum.common.constants import POOL_LEDGER_ID
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.exceptions import SuspiciousNode
# from plenum.common.messages.internal_messages import ViewChangeInProgress, LedgerSyncStatus, ParticipatingStatus
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.common.stashing_router import StashingRouter
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys, updateNamedTuple
from plenum.server.consensus.consensus_data_provider import ConsensusDataProvider
from plenum.server.replica import PP_APPLY_REJECT_WRONG, PP_APPLY_WRONG_DIGEST, PP_APPLY_WRONG_STATE, \
    PP_APPLY_ROOT_HASH_MISMATCH, PP_APPLY_HOOK_ERROR, PP_SUB_SEQ_NO_WRONG, PP_NOT_FINAL, PP_APPLY_AUDIT_HASH_MISMATCH, \
    PP_REQUEST_ALREADY_ORDERED, PP_CHECK_NOT_FROM_PRIMARY, PP_CHECK_TO_PRIMARY, PP_CHECK_DUPLICATE, \
    PP_CHECK_INCORRECT_POOL_STATE_ROOT, PP_CHECK_OLD, PP_CHECK_REQUEST_NOT_FINALIZED, PP_CHECK_NOT_NEXT, \
    PP_CHECK_WRONG_TIME
from plenum.server.replica_validator_enums import INCORRECT_INSTANCE, DISCARD, INCORRECT_PP_SEQ_NO, ALREADY_ORDERED, \
    STASH_VIEW, FUTURE_VIEW, OLD_VIEW, PROCESS, GREATER_PREP_CERT, STASH_CATCH_UP, CATCHING_UP, OUTSIDE_WATERMARKS, \
    STASH_WATERMARKS
from plenum.server.suspicion_codes import Suspicions
from stp_core.common.log import getlogger


class OrdererValidator():
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

    def __init__(self, data: ConsensusDataProvider, bus: InternalBus, network: ExternalBus):
        self._data = data
        self._bus = bus
        self._network = network
        self._config = getConfig()
        self._logger = getlogger()
        self._stasher = StashingRouter(self._config.ORDERING_SERVICE_STASH_LIMIT)
        self._validator = OrdererValidator(self._data)

        """
        Maps from legacy replica code
        """
        self.pre_prepares_stashed_for_incorrect_time = {}

        self._stasher.subscribe(PrePrepare, self.process_preprepare)
        self._stasher.subscribe(Prepare, self.process_prepare)
        self._stasher.subscribe(Commit, self.process_commit)
        self._stasher.subscribe_to(network)

    def process_preprepare(self, pre_prepare: PrePrepare, sender: str):
        result, reason = self._validate(pre_prepare)
        if result == PROCESS:
            self._do_preprepare(pre_prepare, sender)
        return result

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

    def _do_pre_apply_validation(self, pre_prepare: PrePrepare, sender: str):

        def report_suspicious(reason):
            ex = SuspiciousNode(sender, reason, pre_prepare)
            self.report_suspicious_node(ex)

        why_not = self._can_process_pre_prepare(pre_prepare, sender)

        if why_not is None:
            return
        if why_not == PP_CHECK_NOT_FROM_PRIMARY:
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
                req = self.requests.get(key)
                if req is None:
                    absents.add(key)
                elif not req.finalised:
                    non_fin.add(key)
                    non_fin_payload.add(req.request.payload_digest)
            absent_str = ', '.join(str(key) for key in absents)
            non_fin_str = ', '.join(
                '{} ({} : {})'.format(str(key),
                                      str(len(self.requests[key].propagates)),
                                      ', '.join(self.requests[key].propagates.keys())) for key in non_fin)
            self._logger.warning(
                "{} found requests in the incoming pp, of {} ledger, that are not finalized. "
                "{} of them don't have propagates: {}."
                "{} of them don't have enough propagates: {}.".format(self, pre_prepare.ledgerId,
                                                                      len(absents), absent_str,
                                                                      len(non_fin), non_fin_str))

            def signal_suspicious(req):
                self._logger.info("Request digest {} already ordered. Discard {} "
                                 "from {}".format(req, pre_prepare, sender))
                self.report_suspicious(Suspicions.PPR_WITH_ORDERED_REQUEST)

            # checking for payload digest is more effective
            for payload_key in non_fin_payload:
                if self.node.seqNoDB.get_by_payload_digest(payload_key) != (None, None):
                    signal_suspicious(payload_key)
                    return

            # for absents we can only check full digest
            for full_key in absents:
                if self.node.seqNoDB.get_by_full_digest(full_key) is not None:
                    signal_suspicious(full_key)
                    return

            bad_reqs = absents | non_fin
            self.enqueue_pre_prepare(pre_prepare, sender, bad_reqs)
            # TODO: An optimisation might be to not request PROPAGATEs
            # if some PROPAGATEs are present or a client request is
            # present and sufficient PREPAREs and PRE-PREPARE are present,
            # then the digest can be compared but this is expensive as the
            # PREPARE and PRE-PREPARE contain a combined digest
            self._schedule(partial(self.request_propagates_if_needed, bad_reqs, pre_prepare),
                           self._config.PROPAGATE_REQUEST_DELAY)
        elif why_not == PP_CHECK_NOT_NEXT:
            pp_view_no = pre_prepare.viewNo
            pp_seq_no = pre_prepare.ppSeqNo
            last_pp_view_no, last_pp_seq_no = self.__last_pp_3pc
            if pp_view_no >= last_pp_view_no and (
                    self.is_master or self.last_ordered_3pc[1] != 0):
                seq_frm = last_pp_seq_no + 1 if pp_view_no == last_pp_view_no else 1
                seq_to = pp_seq_no - 1
                # Todo: CHK_FREQ should be used from compiled config instead of module
                if seq_to >= seq_frm >= pp_seq_no - self._config.CHK_FREQ + 1:
                    self._logger.warning(
                        "{} missing PRE-PREPAREs from {} to {}, "
                        "going to request".format(self, seq_frm, seq_to))
                    self._request_missing_three_phase_messages(
                        pp_view_no, seq_frm, seq_to)
            self.enqueue_pre_prepare(pre_prepare, sender)
            self._setup_last_ordered_for_non_master()
        elif why_not == PP_CHECK_WRONG_TIME:
            key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
            item = (pre_prepare, sender, False)
            self.pre_prepares_stashed_for_incorrect_time[key] = item
            report_suspicious(Suspicions.PPR_TIME_WRONG)
        elif why_not == BlsBftReplica.PPR_BLS_MULTISIG_WRONG:
            report_suspicious(Suspicions.PPR_BLS_MULTISIG_WRONG)
        else:
            self._logger.warning("Unknown PRE-PREPARE check status: {}".format(why_not))

    def _do_preprepare(self, pre_prepare: PrePrepare, sender: str):
        """
        Validate and process provided PRE-PREPARE, create and
        broadcast PREPARE for it.

        :param pre_prepare: message
        :param sender: name of the node that sent this message
        """
        key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
        self._logger.debug("{} received PRE-PREPARE{} from {}".format(self, key, sender))

        # TODO: should we still do it?
        # Converting each req_idrs from list to tuple
        req_idrs = {f.REQ_IDR.nm: [key for key in pre_prepare.reqIdr]}
        pre_prepare = updateNamedTuple(pre_prepare, **req_idrs)

        cannot_apply = self._do_pre_apply_validation(pre_prepare, sender)
        if cannot_apply is not None:
            return
        why_not_applied = \
            self._process_valid_preprepare(pre_prepare, sender)
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

    def _do_prepare(self, prepare: Prepare, sender: str):
        pass

    def _do_commit(self, commit: Commit, sender: str):
        pass

    @property
    def last_ordered_3pc(self):
        return self._data.last_ordered_3pc

    @property
    def __last_pp_3pc(self):
        last_pp = self.get_last_pp()
        if not last_pp:
            return self.last_ordered_3pc

        last_3pc = (last_pp.viewNo, last_pp.ppSeqNo)
        if compare_3PC_keys(self.last_ordered_3pc, last_3pc) > 0:
            return last_3pc

    @property
    def is_master(self):
        pass

    def get_last_pp(self):
        pass

    def report_suspicious_node(self, ex):
        pass

    def _validate(self, msg):
        return self._validator.validate(msg)

    def _can_process_pre_prepare(self, pre_prepare: PrePrepare, sender: str):
        """
        Decide whether this replica is eligible to process a PRE-PREPARE.

        :param pre_prepare: a PRE-PREPARE msg to process
        :param sender: the name of the node that sent the PRE-PREPARE msg
        """
        # TODO: Check whether it is rejecting PRE-PREPARE from previous view

        # PRE-PREPARE should not be sent from non primary
        if not self.isMsgFromPrimary(pre_prepare, sender):
            return PP_CHECK_NOT_FROM_PRIMARY

        # Already has a PRE-PREPARE with same 3 phase key
        if (pre_prepare.viewNo, pre_prepare.ppSeqNo) in self.prePrepares:
            return PP_CHECK_DUPLICATE

        if not self.is_pre_prepare_time_acceptable(pre_prepare, sender):
            return PP_CHECK_WRONG_TIME

        if compare_3PC_keys((pre_prepare.viewNo, pre_prepare.ppSeqNo),
                            self.__last_pp_3pc) > 0:
            return PP_CHECK_OLD  # ignore old pre-prepare

        if self.nonFinalisedReqs(pre_prepare.reqIdr):
            return PP_CHECK_REQUEST_NOT_FINALIZED

        if not self.__is_next_pre_prepare(pre_prepare.viewNo,
                                          pre_prepare.ppSeqNo):
            return PP_CHECK_NOT_NEXT

        if f.POOL_STATE_ROOT_HASH.nm in pre_prepare and \
                pre_prepare.poolStateRootHash != self.stateRootHash(POOL_LEDGER_ID):
            return PP_CHECK_INCORRECT_POOL_STATE_ROOT

        # BLS multi-sig:
        status = self._bls_bft_replica.validate_pre_prepare(pre_prepare,
                                                            sender)
        if status is not None:
            return status
        return None

    def _process_valid_preprepare(self, pre_prepare: PrePrepare, sender: str):
        pass

    def enqueue_pre_prepare(self, pre_prepare: PrePrepare, sender: str):
        pass

    def request_propagates_if_needed(self, bad_reqs: list, pre_prepare: PrePrepare):
        pass

    def _request_missing_three_phase_messages(self, view_no: int, seq_frm: int, seq_to: int) -> None:
        pass

    def _setup_last_ordered_for_non_master(self):
        pass
