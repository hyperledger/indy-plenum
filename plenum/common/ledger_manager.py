import logging
from collections import Callable
from typing import Any, List, Dict, Tuple, NamedTuple, Iterable
from typing import Optional

from ledger.merkle_verifier import MerkleVerifier
from plenum.common.channel import create_direct_channel, TxChannel, Router
from plenum.common.config_util import getConfig
from plenum.common.constants import LedgerState
from plenum.common.ledger import Ledger
from plenum.common.ledger_info import LedgerInfo
from plenum.common.messages.node_messages import LedgerStatus, CatchupRep, \
    ConsistencyProof, f, CatchupReq
from plenum.common.metrics_collector import MetricsCollector, NullMetricsCollector, measure_time, MetricsName
from plenum.common.util import compare_3PC_keys
from plenum.server.catchup.catchup_rep_service import CatchupRepService, LedgerCatchupComplete
from plenum.server.catchup.cons_proof_service import ConsProofService, ConsProofReady
from plenum.server.catchup.seeder_service import ClientSeederService, NodeSeederService
from plenum.server.catchup.utils import CatchupDataProvider
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


class LedgerManager:
    LedgerLeecherService = NamedTuple('LedgerLeecherService',
                                      [('cons_proof_inbox', TxChannel),
                                       ('cons_proof_service', ConsProofService),
                                       ('catchup_rep_inbox', TxChannel),
                                       ('catchup_rep_service', CatchupRepService)])

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

        self._leecher_outbox, rx = create_direct_channel()
        router = Router(rx)
        router.add(LedgerCatchupComplete, self._on_catchup_rep_service_stop)
        router.add(ConsProofReady, self._on_cons_proof_service_stop)

        self.config = getConfig()

        # Holds ledgers of different types with
        # their info like callbacks, state, etc
        self.ledgerRegistry = {}  # type: Dict[int, LedgerInfo]

        self._leechers = {}   # type: Dict[int, LedgerManager.LedgerLeecherService]

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

        cons_proof_inbox_tx, cons_proof_inbox_rx = create_direct_channel()
        cons_proof_service = ConsProofService(ledger_id=iD,
                                              config=self.config,
                                              input=cons_proof_inbox_rx,
                                              output=self._leecher_outbox,
                                              timer=self._timer,
                                              metrics=self.metrics,
                                              provider=self._provider)

        catchup_rep_inbox_tx, catchup_rep_inbox_rx = create_direct_channel()
        catchup_rep_service = CatchupRepService(ledger_id=iD,
                                                config=self.config,
                                                input=catchup_rep_inbox_rx,
                                                output=self._leecher_outbox,
                                                timer=self._timer,
                                                metrics=self.metrics,
                                                provider=self._provider)

        self._leechers[iD] = self.LedgerLeecherService(cons_proof_inbox=cons_proof_inbox_tx,
                                                       cons_proof_service=cons_proof_service,
                                                       catchup_rep_inbox=catchup_rep_inbox_tx,
                                                       catchup_rep_service=catchup_rep_service)

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

        leecher = self._leechers[ledgerId]
        leecher.cons_proof_inbox.put_nowait((status, frm))

    @staticmethod
    def has_ledger_status_quorum(leger_status_num, total_nodes):
        quorum = Quorums(total_nodes).ledger_status
        return quorum.is_reached(leger_status_num)

    @measure_time(MetricsName.PROCESS_CONSISTENCY_PROOF_TIME)
    def processConsistencyProof(self, proof: ConsistencyProof, frm: str):
        leecher = self._leechers.get(proof.ledgerId)
        if not leecher:
            logger.warning("{} received consistency proof {} for unknown ledger".format(self, proof))
            return

        leecher.cons_proof_inbox.put_nowait((proof, frm))

    @measure_time(MetricsName.PROCESS_CATCHUP_REQ_TIME)
    def processCatchupReq(self, req: CatchupReq, frm: str):
        self._send_to_seeder(req, frm)

    def processCatchupRep(self, rep: CatchupRep, frm: str):
        ledger_id = rep.ledgerId
        leecher = self._leechers.get(ledger_id)
        if not leecher:
            logger.warning("{} received catchup reply {} for unknown ledger".format(self, rep))
            return

        leecher.catchup_rep_inbox.put_nowait((rep, frm))

    def startCatchUpProcess(self, ledgerId: int, proof: ConsistencyProof):
        if ledgerId not in self.ledgerRegistry:
            self.discard(proof, reason="Unknown ledger type {}".
                         format(ledgerId))
            return

        ledgerInfo = self.getLedgerInfoByType(ledgerId)
        ledgerInfo.state = LedgerState.syncing
        ledgerInfo.catchUpTill = proof

        leecher = self._leechers[ledgerId]
        leecher.catchup_rep_service.start(proof)

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
        ledgerInfo.catchUpTill = None

        if self.postAllLedgersCaughtUp:
            if all(l.state == LedgerState.synced
                   for l in self.ledgerRegistry.values()):
                self.postAllLedgersCaughtUp()

        self.catchup_next_ledger(ledgerId)

    def _on_cons_proof_service_stop(self, msg: ConsProofReady):
        self.startCatchUpProcess(msg.ledger_id, msg.cons_proof)

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

            leecher = self._leechers[ledger_id]
            leecher.cons_proof_service.start(request_ledger_statuses)
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

    def getLedgerInfoByType(self, ledgerType) -> LedgerInfo:
        if ledgerType not in self.ledgerRegistry:
            raise KeyError("Invalid ledger type: {}".format(ledgerType))
        return self.ledgerRegistry[ledgerType]

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
