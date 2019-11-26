from typing import Optional, Dict

from plenum.common.channel import RxChannel, TxChannel, Router, create_direct_channel
from plenum.common.constants import LedgerState
from plenum.common.metrics_collector import MetricsCollector
from plenum.common.timer import TimerService
from plenum.server.catchup.catchup_rep_service import CatchupRepService
from plenum.server.catchup.cons_proof_service import ConsProofService
from plenum.server.catchup.utils import CatchupDataProvider, LedgerCatchupStart, LedgerCatchupComplete, CatchupTill
from stp_core.common.log import getlogger

logger = getlogger()


class LedgerLeecherService:
    def __init__(self,
                 ledger_id: int,
                 config: object,
                 input: RxChannel,
                 output: TxChannel,
                 timer: TimerService,
                 metrics: MetricsCollector,
                 provider: CatchupDataProvider):
        self._ledger_id = ledger_id
        self._ledger = provider.ledger(ledger_id)
        self._config = config
        self._output = output
        self._timer = timer
        self.metrics = metrics
        self._provider = provider

        self._state = LedgerState.not_synced  # TODO: Improve enum
        self._catchup_till = None  # type: Optional[CatchupTill]
        self._num_txns_caught_up = 0

        services_tx, services_rx = create_direct_channel()
        router = Router(services_rx)
        router.add(LedgerCatchupStart, self._on_catchup_start)
        router.add(LedgerCatchupComplete, self._on_catchup_complete)

        self._cons_proof_service = ConsProofService(ledger_id=ledger_id,
                                                    config=config,
                                                    input=input,
                                                    output=services_tx,
                                                    timer=self._timer,
                                                    metrics=self.metrics,
                                                    provider=self._provider)

        self._catchup_rep_service = CatchupRepService(ledger_id=ledger_id,
                                                      config=config,
                                                      input=input,
                                                      output=services_tx,
                                                      timer=self._timer,
                                                      metrics=self.metrics,
                                                      provider=self._provider)

    def __repr__(self):
        return "{}:LedgerLeecherService:{}".format(self._provider.node_name(), self._ledger_id)

    @property
    def state(self) -> LedgerState:
        return self._state

    @property
    def catchup_till(self) -> Optional[CatchupTill]:
        return self._catchup_till

    @property
    def num_txns_caught_up(self) -> int:
        return self._num_txns_caught_up

    def start(self,
              request_ledger_statuses: bool = True,
              till: Optional[CatchupTill] = None,
              nodes_ledger_sizes: Optional[Dict[str, int]] = None):

        self._catchup_till = till
        self._num_txns_caught_up = 0
        self._provider.notify_catchup_start(self._ledger_id)
        if till is None:
            self._state = LedgerState.not_synced
            self._cons_proof_service.start(request_ledger_statuses)
        else:
            self._state = LedgerState.syncing
            # TODO: This is an attempt to mimic old behaviour more closely
            if till.start_size == till.final_size:
                till = None
            self._start_catchup(LedgerCatchupStart(ledger_id=self._ledger_id,
                                                   catchup_till=till,
                                                   nodes_ledger_sizes=nodes_ledger_sizes))

    def reset(self):
        self._state = LedgerState.not_synced
        self._catchup_till = None
        self._num_txns_caught_up = 0

    def _on_catchup_start(self, msg: LedgerCatchupStart):
        self._state = LedgerState.syncing
        self._catchup_till = msg.catchup_till
        self._start_catchup(msg)

    def _on_catchup_complete(self, msg: LedgerCatchupComplete):
        self._num_txns_caught_up = msg.num_caught_up
        self._state = LedgerState.synced
        self._catchup_till = None
        self._output.put_nowait(msg)

    def _start_catchup(self, msg: LedgerCatchupStart):
        self._output.put_nowait(msg)
        self._catchup_rep_service.start(msg)
