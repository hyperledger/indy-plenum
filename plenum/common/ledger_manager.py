from collections import Callable
from typing import Any, List, Dict, NamedTuple
from typing import Optional

from ledger.merkle_verifier import MerkleVerifier
from plenum.common.channel import create_direct_channel, TxChannel, Router
from plenum.common.config_util import getConfig
from plenum.common.ledger import Ledger
from plenum.common.ledger_info import LedgerInfo
from plenum.common.messages.node_messages import LedgerStatus, CatchupRep, ConsistencyProof, CatchupReq
from plenum.common.metrics_collector import MetricsCollector, NullMetricsCollector, measure_time, MetricsName
from plenum.common.util import compare_3PC_keys
from plenum.server.catchup.node_catchup_data import CatchupNodeDataProvider
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.server.catchup.seeder_service import ClientSeederService, NodeSeederService
from plenum.server.catchup.utils import LedgerCatchupStart, LedgerCatchupComplete, NodeCatchupComplete
from stp_core.common.log import getlogger

logger = getlogger()


class LedgerManager:
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

        config = getConfig()
        provider = CatchupNodeDataProvider(owner)

        self._client_seeder_inbox, rx = create_direct_channel()
        self._client_seeder = ClientSeederService(rx, provider)

        self._node_seeder_inbox, rx = create_direct_channel()
        self._node_seeder = NodeSeederService(rx, provider)

        leecher_outbox_tx, leecher_outbox_rx = create_direct_channel()
        router = Router(leecher_outbox_rx)
        router.add(LedgerCatchupStart, self._on_ledger_sync_start)
        router.add(LedgerCatchupComplete, self._on_ledger_sync_complete)
        router.add(NodeCatchupComplete, self._on_catchup_complete)

        self._node_leecher_inbox, rx = create_direct_channel()
        self._node_leecher = NodeLeecherService(config=config,
                                                input=rx,
                                                output=leecher_outbox_tx,
                                                timer=self._timer,
                                                metrics=self.metrics,
                                                provider=provider)

        # Holds ledgers of different types with their info like callbacks, state, etc
        self.ledgerRegistry = {}  # type: Dict[int, LedgerInfo]

        # Largest 3 phase key received during catchup.
        # This field is needed to discard any stashed 3PC messages or
        # ordered messages since the transactions part of those messages
        # will be applied when they are received through the catchup process
        self.last_caught_up_3PC = (0, 0)

    def __repr__(self):
        return self.owner.name

    def addLedger(self, ledger_id: int, ledger: Ledger,
                  preCatchupStartClbk: Optional[Callable] = None,
                  postCatchupCompleteClbk: Optional[Callable] = None,
                  postTxnAddedToLedgerClbk: Optional[Callable] = None):

        if ledger_id in self.ledgerRegistry:
            logger.error("{} already present in ledgers so cannot replace that ledger".format(ledger_id))
            return

        self.ledgerRegistry[ledger_id] = LedgerInfo(
            ledger_id,
            ledger=ledger,
            preCatchupStartClbk=preCatchupStartClbk,
            postCatchupCompleteClbk=postCatchupCompleteClbk,
            postTxnAddedToLedgerClbk=postTxnAddedToLedgerClbk,
            verifier=MerkleVerifier(ledger.hasher),
        )

        self._node_leecher.register_ledger(ledger_id)

    def ledger_info(self, lid: int) -> Optional[LedgerInfo]:
        return self.ledgerRegistry.get(lid)

    def start_catchup(self, is_initial: bool = False):
        self._node_leecher.start(is_initial)

    @measure_time(MetricsName.PROCESS_LEDGER_STATUS_TIME)
    def processLedgerStatus(self, status: LedgerStatus, frm: str):
        # TODO: vvv Move this into common LEDGER_STATUS validation
        if status.txnSeqNo < 0:
            return

        ledgerId = status.ledgerId
        if ledgerId not in self.ledgerRegistry:
            return
        # TODO: ^^^

        if self.nodestack.hasRemote(frm):
            self._node_seeder_inbox.put_nowait((status, frm))
            self._node_leecher_inbox.put_nowait((status, frm))
        else:
            self._client_seeder_inbox.put_nowait((status, frm))

    @measure_time(MetricsName.PROCESS_CONSISTENCY_PROOF_TIME)
    def processConsistencyProof(self, proof: ConsistencyProof, frm: str):
        self._node_leecher_inbox.put_nowait((proof, frm))

    @measure_time(MetricsName.PROCESS_CATCHUP_REQ_TIME)
    def processCatchupReq(self, req: CatchupReq, frm: str):
        if self.nodestack.hasRemote(frm):
            self._node_seeder_inbox.put_nowait((req, frm))
        else:
            self._client_seeder_inbox.put_nowait((req, frm))

    @measure_time(MetricsName.PROCESS_CATCHUP_REP_TIME)
    def processCatchupRep(self, rep: CatchupRep, frm: str):
        self._node_leecher_inbox.put_nowait((rep, frm))

    def _on_ledger_sync_start(self, msg: LedgerCatchupStart):
        pass

    def _on_ledger_sync_complete(self, msg: LedgerCatchupComplete):
        pass

    def _on_catchup_complete(self, _: NodeCatchupComplete):
        if self.postAllLedgersCaughtUp:
            self.postAllLedgersCaughtUp()

    def sendTo(self, msg: Any, to: str, message_splitter=None):
        if self.nodestack.hasRemote(to):
            self.owner.sendToNodes(msg, [to, ], message_splitter)
        else:
            self.owner.transmitToClient(msg, to)

    @property
    def nodestack(self):
        return self.owner.nodestack

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
