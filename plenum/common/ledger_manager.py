import heapq
import logging
import math
import operator
import time
from collections import Callable, Counter
from functools import partial
from random import shuffle
from typing import Any, List, Dict, Tuple, NamedTuple, Iterable
from typing import Optional

from ledger.merkle_verifier import MerkleVerifier
from plenum.common.channel import create_direct_channel, TxChannel, Router
from plenum.common.config_util import getConfig
from plenum.common.constants import POOL_LEDGER_ID, LedgerState, CONSISTENCY_PROOF, CATCH_UP_PREFIX
from plenum.common.ledger import Ledger
from plenum.common.ledger_info import LedgerInfo
from plenum.common.messages.node_messages import LedgerStatus, CatchupRep, \
    ConsistencyProof, f, CatchupReq
from plenum.common.metrics_collector import MetricsCollector, NullMetricsCollector, measure_time, MetricsName
from plenum.common.util import compare_3PC_keys, SortedDict, min_3PC_key
from plenum.server.catchup.catchup_rep_service import CatchupRepService, LedgerCatchupComplete
from plenum.server.catchup.seeder_service import ClientSeederService, NodeSeederService
from plenum.server.catchup.utils import CatchupDataProvider
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.quorums import Quorums
from stp_core.common.log import getlogger

logger = getlogger()


class CatchupNodeDataProvider(CatchupDataProvider):
    def __init__(self, node):
        self._node = node

    def node_name(self) -> str:
        return self._node.name

    def all_nodes_names(self) -> List[str]:
        return self._node.allNodeNames

    def ledgers(self) -> List[int]:
        return self._node.ledger_ids

    def ledger(self, ledger_id: int) -> Ledger:
        info = self._ledger_info(ledger_id)
        return info.ledger if info is not None else None

    def verifier(self, ledger_id: int) -> MerkleVerifier:
        info = self._ledger_info(ledger_id)
        return info.verifier if info is not None else None

    def eligible_nodes(self) -> List[str]:
        return self._node.ledgerManager.nodes_to_request_txns_from

    def three_phase_key_for_txn_seq_no(self, ledger_id: int, seq_no: int) -> Tuple[int, int]:
        return self._node.three_phase_key_for_txn_seq_no(ledger_id, seq_no)

    def update_txn_with_extra_data(self, txn: dict) -> dict:
        return self._node.update_txn_with_extra_data(txn)

    def transform_txn_for_ledger(self, txn: dict) -> dict:
        return self._node.transform_txn_for_ledger(txn)

    def notify_catchup_start(self, ledger_id: int):
        if self._node.ledgerManager.preCatchupClbk:
            self._node.ledgerManager.preCatchupClbk(ledger_id)

        info = self._ledger_info(ledger_id)
        if info is not None and info.preCatchupStartClbk:
            info.preCatchupStartClbk()

    def notify_catchup_complete(self, ledger_id: int, last_3pc: Tuple[int, int]):
        if self._node.ledgerManager.postCatchupClbk:
            self._node.ledgerManager.postCatchupClbk(ledger_id, last_3pc)

        info = self._ledger_info(ledger_id)
        if info is not None and info.postCatchupCompleteClbk:
            info.postCatchupCompleteClbk()

    def notify_transaction_added_to_ledger(self, ledger_id: int, txn: dict):
        info = self._ledger_info(ledger_id)
        if info is not None and info.postTxnAddedToLedgerClbk:
            info.postTxnAddedToLedgerClbk(ledger_id, txn)

    def send_to(self, msg: Any, to: str, message_splitter: Optional[Callable] = None):
        if self._node.nodestack.hasRemote(to):
            self._node.sendToNodes(msg, [to], message_splitter)
        else:
            self._node.transmitToClient(msg, to)

    def send_to_nodes(self, msg: Any, nodes: Iterable[str] = None):
        self._node.sendToNodes(msg, nodes)

    def blacklist_node(self, node_name: str, reason: str):
        self._node.blacklistNode(node_name, reason)

    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        self._node.discard(msg, reason, logMethod, cliOutput)

    def _ledger_info(self, ledger_id: int):
        return self._node.ledgerManager.ledgerRegistry.get(ledger_id)


class LedgerManager(HasActionQueue):
    CatchupRepServiceWrapper = NamedTuple('CatchupRepServiceWrapper',
                                          [('inbox', TxChannel), ('service', CatchupRepService)])

    def __init__(self,
                 owner,
                 postAllLedgersCaughtUp: Optional[Callable] = None,
                 preCatchupClbk: Optional[Callable] = None,
                 postCatchupClbk: Optional[Callable] = None,
                 ledger_sync_order: Optional[List] = None,
                 metrics: MetricsCollector = NullMetricsCollector()):
        # If ledger_sync_order is not provided (is None), it is assumed that
        # `postCatchupCompleteClbk` of the LedgerInfo will be used
        self.owner = owner
        self._timer = owner.timer
        self.postAllLedgersCaughtUp = postAllLedgersCaughtUp
        self.preCatchupClbk = preCatchupClbk
        self.postCatchupClbk = postCatchupClbk
        self.ledger_sync_order = ledger_sync_order
        self.request_ledger_status_action_ids = dict()
        self.request_consistency_proof_action_ids = dict()
        self.metrics = metrics

        self._provider = CatchupNodeDataProvider(owner)

        self._client_seeder_inbox, rx = create_direct_channel()
        self._client_seeder = ClientSeederService(rx, self._provider)

        self._node_seeder_inbox, rx = create_direct_channel()
        self._node_seeder = NodeSeederService(rx, self._provider)

        self._catchup_rep_outbox, rx = create_direct_channel()
        Router(rx).add(LedgerCatchupComplete, self._on_catchup_rep_service_stop)

        self.config = getConfig()
        # Needs to schedule actions. The owner of the manager has the
        # responsibility of calling its `_serviceActions` method periodically
        HasActionQueue.__init__(self)

        # Holds ledgers of different types with
        # their info like callbacks, state, etc
        self.ledgerRegistry = {}  # type: Dict[int, LedgerInfo]

        self._catchup_rep_services = {}   # type: Dict[int, LedgerManager.CatchupRepWrapper]

        # Largest 3 phase key received during catchup.
        # This field is needed to discard any stashed 3PC messages or
        # ordered messages since the transactions part of those messages
        # will be applied when they are received through the catchup process
        self.last_caught_up_3PC = (0, 0)

        # Nodes are added in this set when the current node sent a CatchupReq
        # for them and waits a CatchupRep message.
        self.wait_catchup_rep_from = set()

    def __repr__(self):
        return self.owner.name

    @measure_time(MetricsName.SERVICE_LEDGER_MANAGER_TIME)
    def service(self):
        return self._serviceActions()

    def addLedger(self, iD: int, ledger: Ledger,
                  preCatchupStartClbk: Callable = None,
                  postCatchupCompleteClbk: Callable = None,
                  postTxnAddedToLedgerClbk: Callable = None):

        if iD in self.ledgerRegistry:
            logger.error("{} already present in ledgers "
                         "so cannot replace that ledger".format(iD))
            return

        self.ledgerRegistry[iD] = LedgerInfo(
            iD,
            ledger=ledger,
            preCatchupStartClbk=preCatchupStartClbk,
            postCatchupCompleteClbk=postCatchupCompleteClbk,
            postTxnAddedToLedgerClbk=postTxnAddedToLedgerClbk,
            verifier=MerkleVerifier(ledger.hasher)
        )

        inbox_tx, inbox_rx = create_direct_channel()
        service = CatchupRepService(ledger_id=iD,
                                    config=self.config,
                                    input=inbox_rx,
                                    output=self._catchup_rep_outbox,
                                    timer=self._timer,
                                    metrics=self.metrics,
                                    provider=self._provider)
        self._catchup_rep_services[iD] = self.CatchupRepServiceWrapper(inbox=inbox_tx, service=service)

    def _cancel_request_ledger_statuses_and_consistency_proofs(self, ledger_id):
        if ledger_id in self.request_ledger_status_action_ids:
            action_id = self.request_ledger_status_action_ids.pop(ledger_id)
            self._cancel(aid=action_id)

        if ledger_id in self.request_consistency_proof_action_ids:
            action_id = self.request_consistency_proof_action_ids.pop(ledger_id)
            self._cancel(aid=action_id)

    def reask_for_ledger_status(self, ledger_id):
        self.request_ledger_status_action_ids.pop(ledger_id, None)
        ledgerInfo = self.getLedgerInfoByType(ledger_id)
        nodes = [node for node in self.owner.nodeReg if node not in ledgerInfo.ledgerStatusOk]
        self.owner.request_ledger_status_from_nodes(ledger_id, nodes)

    def reask_for_last_consistency_proof(self, ledger_id):
        self.request_consistency_proof_action_ids.pop(ledger_id, None)
        ledgerInfo = self.getLedgerInfoByType(ledger_id)
        recvdConsProof = ledgerInfo.recvdConsistencyProofs
        ledger_status = self.owner.build_ledger_status(ledger_id)
        nodes = [frm for frm in self.owner.nodeReg if
                 frm not in recvdConsProof and frm != self.owner.name]
        for frm in nodes:
                self.sendTo(ledger_status, frm)

    def request_CPs_if_needed(self, ledgerId):
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        if ledgerInfo.consistencyProofsTimer is None:
            return

        proofs = ledgerInfo.recvdConsistencyProofs
        # there is no any received ConsistencyProofs
        if not proofs:
            return
        logger.info("{} requesting consistency proofs after timeout".format(self))

        quorum = Quorums(self.owner.totalNodes)
        groupedProofs, null_proofs_count = self._groupConsistencyProofs(proofs)
        if quorum.ledger_status.is_reached(null_proofs_count) \
                or len(groupedProofs) == 0:
            return
        result = self._latestReliableProof(groupedProofs, ledgerInfo.ledger)
        if not result:
            ledger_id, start, end = self.get_consistency_proof_request_params(
                ledgerId, groupedProofs)
            logger.info("{} sending consistency proof request: {}".format(self, ledger_id, start, end))
            self.owner.request_msg(CONSISTENCY_PROOF,
                                   {f.LEDGER_ID.nm: ledger_id,
                                    f.SEQ_NO_START.nm: start,
                                    f.SEQ_NO_END.nm: end},
                                   self.nodes_to_request_txns_from)

        ledgerInfo.recvdConsistencyProofs = {}
        ledgerInfo.consistencyProofsTimer = None

    def setLedgerState(self, ledgerType: int, state: LedgerState):
        if ledgerType not in self.ledgerRegistry:
            logger.error("ledger type {} not present in ledgers so "
                         "cannot set state".format(ledgerType))
            return
        self.getLedgerInfoByType(ledgerType).state = state

    def setLedgerCanSync(self, ledgerType: int, canSync: bool):
        try:
            ledger_info = self.getLedgerInfoByType(ledgerType)
            ledger_info.canSync = canSync
        except KeyError:
            logger.error("ledger type {} not present in ledgers so "
                         "cannot set state".format(ledgerType))
            return

    def prepare_ledgers_for_sync(self):
        for ledger_info in self.ledgerRegistry.values():
            ledger_info.set_defaults()

    @measure_time(MetricsName.PROCESS_LEDGER_STATUS_TIME)
    def processLedgerStatus(self, status: LedgerStatus, frm: str):
        self._send_to_seeder(status, frm)

        # If the ledger status is from client then we do nothing more
        if self.getStack(frm) == self.clientstack:
            return

        # TODO: vvv Move this into common LEDGER_STATUS validation
        if status.txnSeqNo < 0:
            return

        ledgerId = status.ledgerId
        if ledgerId not in self.ledgerRegistry:
            return
        # TODO: ^^^

        ledgerStatus = status
        ledgerInfo = self.getLedgerInfoByType(status.ledgerId)

        # If we are performing a catch-up of the corresponding ledger
        if ledgerInfo.state == LedgerState.not_synced and ledgerInfo.canSync:
            # If we are behind the node which has sent the ledger status
            # then send our ledger status to it
            # in order to get the consistency proof from it
            if self.isLedgerOld(ledgerStatus):
                ledger_status = self.owner.build_ledger_status(ledgerId)
                self.sendTo(ledger_status, frm)
                if ledgerId not in self.request_consistency_proof_action_ids:
                    self.request_consistency_proof_action_ids[ledgerId] = \
                        self._schedule(
                            partial(self.reask_for_last_consistency_proof, ledgerId),
                            self.config.ConsistencyProofsTimeout * (self.owner.totalNodes - 1))
                return

            # We are not behind the node which has sent the ledger status,
            # so our ledger is OK in comparison with that node
            # and we will not get a consistency proof from it
            ledgerInfo.ledgerStatusOk.add(frm)
            ledgerInfo.recvdConsistencyProofs[frm] = None

            # If we are even with the node which has sent the ledger status
            # then save the last txn master 3PC-key from it
            if self.isLedgerSame(ledgerStatus):
                ledgerInfo.last_txn_3PC_key[frm] = \
                    (ledgerStatus.viewNo, ledgerStatus.ppSeqNo)

            # If we gathered the quorum of ledger statuses indicating
            # that our ledger is OK then we do not need to perform actual
            # synchronization of our ledger (however, we still need to do
            # the ledger pre-catchup and post-catchup procedures)
            if self.has_ledger_status_quorum(
                    len(ledgerInfo.ledgerStatusOk), self.owner.totalNodes):
                logger.info("{} found out from {} that its ledger of type {} is latest".
                            format(self, ledgerInfo.ledgerStatusOk, ledgerId))
                logger.info('{} found from ledger status {} that it does not need catchup'.
                            format(self, ledgerStatus))
                # Stop requesting last consistency proofs and ledger statuses.
                self._cancel_request_ledger_statuses_and_consistency_proofs(ledgerId)
                self.do_pre_catchup(ledgerId)
                last_3PC_key = self._get_last_txn_3PC_key(ledgerInfo)
                self.catchupCompleted(ledgerId, last_3PC_key)

    def _get_last_txn_3PC_key(self, ledgerInfo):
        quorum = Quorums(self.owner.totalNodes)
        quorumed_3PC_keys = \
            [
                most_common_element
                for most_common_element, freq in
                Counter(ledgerInfo.last_txn_3PC_key.values()).most_common()
                if quorum.ledger_status_last_3PC.is_reached(freq) and
                most_common_element[0] is not None and
                most_common_element[1] is not None
            ]

        if len(quorumed_3PC_keys) == 0:
            return None

        min_quorumed_3PC_key = min_3PC_key(quorumed_3PC_keys)
        return min_quorumed_3PC_key

    @staticmethod
    def has_ledger_status_quorum(leger_status_num, total_nodes):
        quorum = Quorums(total_nodes).ledger_status
        return quorum.is_reached(leger_status_num)

    @measure_time(MetricsName.PROCESS_CONSISTENCY_PROOF_TIME)
    def processConsistencyProof(self, proof: ConsistencyProof, frm: str):
        logger.info("{} received consistency proof: {} from {}".format(self, proof, frm))
        ledgerId = getattr(proof, f.LEDGER_ID.nm)
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        ledgerInfo.recvdConsistencyProofs[frm] = ConsistencyProof(*proof)

        if self.canProcessConsistencyProof(proof):
            canCatchup, catchUpFrm = self.canStartCatchUpProcess(ledgerId)
            if canCatchup:
                self.startCatchUpProcess(ledgerId, catchUpFrm)

    def canProcessConsistencyProof(self, proof: ConsistencyProof) -> bool:
        ledgerId = getattr(proof, f.LEDGER_ID.nm)
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        if not ledgerInfo.canSync:
            logger.info("{} cannot process consistency proof since canSync is {}".
                        format(self, ledgerInfo.canSync))
            return False
        if ledgerInfo.state != LedgerState.not_synced:
            logger.info("{} cannot process consistency proof since ledger state is {}".
                        format(self, ledgerInfo.state))
            return False

        start = getattr(proof, f.SEQ_NO_START.nm)
        end = getattr(proof, f.SEQ_NO_END.nm)
        # TODO: Should we discard where start is older than the ledger size
        ledgerSize = ledgerInfo.ledger.size
        if start > ledgerSize:
            self.discard(proof, reason="Start {} is greater than "
                                       "ledger size {}".
                         format(start, ledgerSize),
                         logMethod=logger.warning)
            return False
        if end <= start:
            self.discard(proof, reason="End {} is not greater than "
                                       "start {}".format(end, start),
                         logMethod=logger.warning)
            return False
        return True

    @measure_time(MetricsName.PROCESS_CATCHUP_REQ_TIME)
    def processCatchupReq(self, req: CatchupReq, frm: str):
        self._send_to_seeder(req, frm)

    def processCatchupRep(self, rep: CatchupRep, frm: str):
        ledger_id = rep.ledgerId
        wrapper = self._catchup_rep_services.get(ledger_id)
        if not wrapper:
            logger.warning("{} received catchup reply {} for unknown ledger".format(self, rep))
            return

        wrapper.inbox.put_nowait((rep, frm))

    # ASSUMING NO MALICIOUS NODES
    # Assuming that all nodes have the same state of the system and no node
    # is lagging behind. So if two new nodes are added in quick succession in a
    # high traffic environment, this logic is faulty
    def canStartCatchUpProcess(self, ledgerId: int):
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        recvdConsProof = ledgerInfo.recvdConsistencyProofs
        # Consider an f value when this node was not connected
        adjustedQuorum = Quorums(self.owner.totalNodes)
        if len([v for v in recvdConsProof.values() if v is not None]) == \
                adjustedQuorum.f + 1:
            # At least once correct node believes that this node is behind.

            # Stop requesting last consistency proofs and ledger statuses.
            self._cancel_request_ledger_statuses_and_consistency_proofs(ledgerId)

            # Start timer that will expire in some time and if till that time
            # enough CPs are not received, then explicitly request CPs
            # from other nodes, see `request_CPs_if_needed`

            ledgerInfo.consistencyProofsTimer = time.perf_counter()
            # TODO: find appropriate moment to unschedule this event!
            self._schedule(partial(self.request_CPs_if_needed, ledgerId),
                           self.config.ConsistencyProofsTimeout * (
                               self.owner.totalNodes - 1))
        if adjustedQuorum.consistency_proof.is_reached(len(recvdConsProof)):
            logger.info("{} deciding on the basis of CPs {} and f {}".
                        format(self, recvdConsProof, adjustedQuorum.f))
            grpdPrf, null_proofs_count = self._groupConsistencyProofs(
                recvdConsProof)
            # If at least n-f-1 nodes were found to be at the same state
            # then this node's state is good too
            if adjustedQuorum.ledger_status.is_reached(null_proofs_count):
                return True, None
            result = self._latestReliableProof(grpdPrf,
                                               ledgerInfo.ledger)
            cp = ConsistencyProof(ledgerId, *result) if result else None
            return bool(result), cp

        logger.info("{} cannot start catchup since received only {} "
                    "consistency proofs but need at least {}".
                    format(self, len(recvdConsProof), adjustedQuorum.consistency_proof.value))
        return False, None

    def _groupConsistencyProofs(self, proofs):
        recvdPrf = {}
        # For the case where the other node is at the same state as
        # this node
        nullProofs = 0
        for nodeName, proof in proofs.items():
            if proof:
                start, end = getattr(proof, f.SEQ_NO_START.nm), \
                    getattr(proof, f.SEQ_NO_END.nm)
                if (start, end) not in recvdPrf:
                    recvdPrf[(start, end)] = {}
                key = (
                    getattr(proof, f.VIEW_NO.nm),
                    getattr(proof, f.PP_SEQ_NO.nm),
                    getattr(proof, f.OLD_MERKLE_ROOT.nm),
                    getattr(proof, f.NEW_MERKLE_ROOT.nm),
                    tuple(getattr(proof, f.HASHES.nm))
                )
                recvdPrf[(start, end)][key] = recvdPrf[(start, end)]. \
                    get(key, 0) + 1
            else:
                logger.info("{} found proof by {} null".format(self, nodeName))
                nullProofs += 1
        return recvdPrf, nullProofs

    def _reliableProofs(self, groupedProofs):
        adjustedQuorum = Quorums(self.owner.totalNodes)
        result = {}
        for (start, end), val in groupedProofs.items():
            for (view_no, lastPpSeqNo, oldRoot,
                 newRoot, hashes), count in val.items():
                if adjustedQuorum.same_consistency_proof.is_reached(count):
                    result[(start, end)] = (view_no, lastPpSeqNo, oldRoot,
                                            newRoot, hashes)
                    # There would be only one correct proof for a range of
                    # sequence numbers
                    break
        return result

    def _latestReliableProof(self, groupedProofs, ledger):
        reliableProofs = self._reliableProofs(groupedProofs)
        latest = None
        for (start, end), (view_no, last_pp_seq_no, oldRoot,
                           newRoot, hashes) in reliableProofs.items():
            # TODO: Can we do something where consistency proof's start is older
            #  than the current ledger's size and proof's end is larger
            # than the current ledger size.
            # Ignore if proof's start is not the same as the ledger's end
            if start != ledger.size:
                continue
            if latest is None or latest[1] < end:
                latest = (start, end) + (view_no, last_pp_seq_no,
                                         oldRoot, newRoot, hashes)
        return latest

    def get_consistency_proof_request_params(self, ledgerId, groupedProofs):
        # Choose the consistency proof which occurs median number of times in
        # grouped proofs. Not choosing the highest since some malicious nodes
        # might be sending non-existent sequence numbers and not choosing the
        # lowest since that might not be enough as some nodes must be lagging
        # behind a lot or some malicious nodes might send low sequence numbers.
        proofs = sorted(groupedProofs.items(),
                        key=lambda t: max(t[1].values()))
        ledger = self.getLedgerInfoByType(ledgerId).ledger
        return ledgerId, ledger.size, proofs[len(proofs) // 2][0][1]

    def do_pre_catchup(self, ledger_id):
        if self.preCatchupClbk:
            self.preCatchupClbk(ledger_id)
        ledgerInfo = self.getLedgerInfoByType(ledger_id)
        ledgerInfo.pre_syncing()

    def startCatchUpProcess(self, ledgerId: int, proof: ConsistencyProof):
        if ledgerId not in self.ledgerRegistry:
            self.discard(proof, reason="Unknown ledger type {}".
                         format(ledgerId))
            return

        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        ledgerInfo.state = LedgerState.syncing
        ledgerInfo.catchUpTill = proof

        wrapper = self._catchup_rep_services[ledgerId]
        wrapper.service.start(proof)

    def catchupCompleted(self, ledgerId: int, last_3PC: Optional[Tuple] = None):
        # Since multiple ledger will be caught up and catchups might happen
        # multiple times for a single ledger, the largest seen
        # ppSeqNo needs to be known.
        if last_3PC is not None \
                and compare_3PC_keys(self.last_caught_up_3PC, last_3PC) > 0:
            self.last_caught_up_3PC = last_3PC

        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        ledgerInfo.canSync = False
        ledgerInfo.state = LedgerState.synced
        ledgerInfo.ledgerStatusOk = set()
        ledgerInfo.last_txn_3PC_key = {}
        ledgerInfo.recvdConsistencyProofs = {}
        ledgerInfo.catchUpTill = None

        if self.postAllLedgersCaughtUp:
            if all(l.state == LedgerState.synced
                   for l in self.ledgerRegistry.values()):
                self.postAllLedgersCaughtUp()

        self.catchup_next_ledger(ledgerId)

    def _on_catchup_rep_service_stop(self, msg: LedgerCatchupComplete):
        self.getLedgerInfoByType(msg.ledger_id).num_txns_caught_up = msg.num_caught_up
        self.catchupCompleted(msg.ledger_id, msg.last_3pc)

    def catchup_next_ledger(self, ledger_id):
        next_ledger_id = self.ledger_to_sync_after(ledger_id)
        if next_ledger_id is not None:
            self.catchup_ledger(next_ledger_id)
        else:
            logger.info('{} not found any ledger to catchup after {}'.format(self, ledger_id))

    def catchup_ledger(self, ledger_id, request_ledger_statuses=True):
        try:
            ledger_info = self.getLedgerInfoByType(ledger_id)
            ledger_info.set_defaults()
            ledger_info.canSync = True
            if request_ledger_statuses:
                self.owner.request_ledger_status_from_nodes(ledger_id)
                self.request_ledger_status_action_ids[ledger_id] = \
                    self._schedule(
                        partial(self.reask_for_ledger_status,
                                ledger_id),
                        self.config.LedgerStatusTimeout * (self.owner.totalNodes - 1))
        except KeyError:
            logger.error("ledger type {} not present in ledgers so "
                         "cannot set state".format(ledger_id))
            return

    def ledger_to_sync_after(self, ledger_id) -> Optional[int]:
        if self.ledger_sync_order:
            try:
                idx = self.ledger_sync_order.index(ledger_id)
                if idx < (len(self.ledger_sync_order) - 1):
                    return self.ledger_sync_order[idx + 1]
            except ValueError:
                return None

    def getConsistencyProof(self, status: LedgerStatus):
        ledger = self.getLedgerForMsg(status)  # type: Ledger
        ledgerId = getattr(status, f.LEDGER_ID.nm)
        seqNoStart = getattr(status, f.TXN_SEQ_NO.nm)
        seqNoEnd = ledger.size
        return self._buildConsistencyProof(ledgerId, seqNoStart, seqNoEnd)

    # TODO: Replace with CatchupDataProvider
    def _buildConsistencyProof(self, ledgerId, seqNoStart, seqNoEnd):
        ledger = self.getLedgerInfoByType(ledgerId).ledger

        ledgerSize = ledger.size
        if seqNoStart > ledgerSize:
            logger.warning(
                "{} cannot build consistency proof from {} "
                "since its ledger size is {}".format(self, seqNoStart, ledgerSize))
            return
        if seqNoEnd > ledgerSize:
            logger.warning(
                "{} cannot build consistency "
                "proof till {} since its ledger size is {}".format(self, seqNoEnd, ledgerSize))
            return
        if seqNoEnd < seqNoStart:
            logger.warning(
                '{} cannot build consistency proof since end {} is '
                'lesser than start {}'.format(self, seqNoEnd, seqNoStart))
            return

        if seqNoStart == 0:
            # Consistency proof for an empty tree cannot exist. Using the root
            # hash now so that the node which is behind can verify that
            # TODO: Make this an empty list
            oldRoot = ledger.tree.root_hash
            proof = [oldRoot, ]
        else:
            proof = ledger.tree.consistency_proof(seqNoStart, seqNoEnd)
            oldRoot = ledger.tree.merkle_tree_hash(0, seqNoStart)

        newRoot = ledger.tree.merkle_tree_hash(0, seqNoEnd)
        key = self.owner.three_phase_key_for_txn_seq_no(ledgerId, seqNoEnd)
        logger.info('{} found 3 phase key {} for ledger {} seqNo {}'.format(self, key, ledgerId, seqNoEnd))
        if key is None:
            # The node receiving consistency proof should check if it has
            # received this sentinel 3 phase key (0, 0) in spite of seeing a
            # non-zero txn seq no
            key = (0, 0)

        return ConsistencyProof(
            ledgerId,
            seqNoStart,
            seqNoEnd,
            *key,
            Ledger.hashToStr(oldRoot),
            Ledger.hashToStr(newRoot),
            [Ledger.hashToStr(p) for p in proof]
        )

    def _compareLedger(self, status: LedgerStatus):
        ledgerId = getattr(status, f.LEDGER_ID.nm)
        seqNo = getattr(status, f.TXN_SEQ_NO.nm)
        ledger = self.getLedgerForMsg(status)
        logger.info("{} comparing its ledger {} of size {} with {}".
                    format(self, ledgerId, ledger.seqNo, seqNo))
        return ledger.seqNo - seqNo

    def isLedgerOld(self, status: LedgerStatus):
        # Is self ledger older than the `LedgerStatus`
        return self._compareLedger(status) < 0

    def isLedgerNew(self, status: LedgerStatus):
        # Is self ledger newer than the `LedgerStatus`
        return self._compareLedger(status) > 0

    def isLedgerSame(self, status: LedgerStatus):
        # Is self ledger same as the `LedgerStatus`
        return self._compareLedger(status) == 0

    def getLedgerForMsg(self, msg: Any) -> Ledger:
        ledger_id = getattr(msg, f.LEDGER_ID.nm)
        if ledger_id in self.ledgerRegistry:
            return self.getLedgerInfoByType(ledger_id).ledger
        self.discard(msg, reason="Invalid ledger msg type")

    def getLedgerInfoByType(self, ledgerType) -> LedgerInfo:
        if ledgerType not in self.ledgerRegistry:
            raise KeyError("Invalid ledger type: {}".format(ledgerType))
        return self.ledgerRegistry[ledgerType]

    def appendToLedger(self, ledgerId: int, txn: Any) -> Dict:
        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        return ledgerInfo.ledger.append(txn)

    def _send_to_seeder(self, msg: Any, frm: str):
        if self.nodestack.hasRemote(frm):
            self._node_seeder_inbox.put_nowait((msg, frm))
        else:
            self._client_seeder_inbox.put_nowait((msg, frm))

    def getStack(self, remoteName: str):
        if self.nodestack.hasRemote(remoteName):
            return self.nodestack
        else:
            return self.clientstack

    def sendTo(self, msg: Any, to: str, message_splitter=None):
        stack = self.getStack(to)
        if stack == self.nodestack:
            self.sendToNodes(msg, [to, ], message_splitter)
        if stack == self.clientstack:
            self.owner.transmitToClient(msg, to)

    def send_catchup_req(self, msg: CatchupReq, to: str):
        self.wait_catchup_rep_from.add(to)
        self.sendTo(msg, to)

    @property
    def nodestack(self):
        return self.owner.nodestack

    @property
    def clientstack(self):
        return self.owner.clientstack

    @property
    def send(self):
        return self.owner.send

    @property
    def sendToNodes(self):
        return self.owner.sendToNodes

    @property
    def discard(self):
        return self.owner.discard

    @property
    def blacklistedNodes(self):
        return self.owner.blacklistedNodes

    @property
    def nodes_to_request_txns_from(self):
        nodes_list = self.nodestack.connecteds \
            if self.nodestack.connecteds \
            else self.nodestack.registry
        return [nm for nm in nodes_list
                if nm not in self.blacklistedNodes and nm != self.nodestack.name]
