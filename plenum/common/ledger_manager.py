from collections import Callable
from typing import Any, List, Dict, NamedTuple
from typing import Optional

from ledger.merkle_verifier import MerkleVerifier
from plenum.common.channel import create_direct_channel, TxChannel, Router
from plenum.common.config_util import getConfig
from plenum.common.constants import LedgerState
from plenum.common.ledger import Ledger
from plenum.common.ledger_info import LedgerInfo
from plenum.common.messages.node_messages import LedgerStatus, CatchupRep, ConsistencyProof, CatchupReq
from plenum.common.metrics_collector import MetricsCollector, NullMetricsCollector, measure_time, MetricsName
from plenum.common.util import compare_3PC_keys
from plenum.server.catchup.catchup_rep_service import LedgerCatchupComplete
from plenum.server.catchup.ledger_leecher_service import LedgerLeecherService
from plenum.server.catchup.node_catchup_data import CatchupNodeDataProvider
from plenum.server.catchup.seeder_service import ClientSeederService, NodeSeederService
from stp_core.common.log import getlogger

logger = getlogger()


class LedgerManager:
    LedgerLeecher = NamedTuple('LedgerLeecher', [('inbox', TxChannel), ('service', LedgerLeecherService)])

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
        Router(rx).add(LedgerCatchupComplete, self._on_leecher_service_stop)

        self.config = getConfig()

        # Holds ledgers of different types with their info like callbacks, state, etc
        self.ledgerRegistry = {}  # type: Dict[int, LedgerInfo]

        # Holds ledger leecher services
        self._leechers = {}   # type: Dict[int, LedgerManager.LedgerLeecher]

        # Largest 3 phase key received during catchup.
        # This field is needed to discard any stashed 3PC messages or
        # ordered messages since the transactions part of those messages
        # will be applied when they are received through the catchup process
        self.last_caught_up_3PC = (0, 0)

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

        inbox_tx, inbox_rx = create_direct_channel()
        service = LedgerLeecherService(ledger_id=iD,
                                       config=self.config,
                                       input=inbox_rx,
                                       output=self._leecher_outbox,
                                       timer=self._timer,
                                       metrics=self.metrics,
                                       provider=self._provider)
        self._leechers[iD] = self.LedgerLeecher(inbox=inbox_tx, service=service)

    def start_catchup(self, request_ledger_statuses: bool):
        for leecher in self._leechers.values():
            leecher.service.reset()
        self.catchup_ledger(self.ledger_sync_order[0], request_ledger_statuses)

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

        self._leechers[ledgerId].inbox.put_nowait((status, frm))

    @measure_time(MetricsName.PROCESS_CONSISTENCY_PROOF_TIME)
    def processConsistencyProof(self, proof: ConsistencyProof, frm: str):
        leecher = self._leechers.get(proof.ledgerId)
        if not leecher:
            logger.warning("{} received consistency proof {} for unknown ledger".format(self, proof))
            return

        leecher.inbox.put_nowait((proof, frm))

    @measure_time(MetricsName.PROCESS_CATCHUP_REQ_TIME)
    def processCatchupReq(self, req: CatchupReq, frm: str):
        self._send_to_seeder(req, frm)

    def processCatchupRep(self, rep: CatchupRep, frm: str):
        ledger_id = rep.ledgerId
        leecher = self._leechers.get(ledger_id)
        if not leecher:
            logger.warning("{} received catchup reply {} for unknown ledger".format(self, rep))
            return

        leecher.inbox.put_nowait((rep, frm))

    def _on_leecher_service_stop(self, msg: LedgerCatchupComplete):
        if msg.last_3pc is not None and compare_3PC_keys(self.last_caught_up_3PC, msg.last_3pc) > 0:
            self.last_caught_up_3PC = msg.last_3pc

        if all(leecher.service.state == LedgerState.synced for leecher in self._leechers.values()):
            if self.postAllLedgersCaughtUp:
                self.postAllLedgersCaughtUp()
            # TODO: Should we exit here?

        self.catchup_next_ledger(msg.ledger_id)

    def catchup_next_ledger(self, ledger_id):
        next_ledger_id = self.ledger_to_sync_after(ledger_id)
        if next_ledger_id is not None:
            self.catchup_ledger(next_ledger_id)
        else:
            logger.info('{} not found any ledger to catchup after {}'.format(self, ledger_id))

    def catchup_ledger(self, ledger_id, request_ledger_statuses=True):
        try:
            leecher = self._leechers[ledger_id]
            leecher.service.start(request_ledger_statuses)
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
