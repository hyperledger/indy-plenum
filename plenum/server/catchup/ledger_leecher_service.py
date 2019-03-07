from typing import Optional

from plenum.common.channel import RxChannel, TxChannel, Router, create_direct_channel
from plenum.common.messages.node_messages import ConsistencyProof
from plenum.common.metrics_collector import MetricsCollector
from plenum.common.timer import TimerService
from plenum.server.catchup.catchup_rep_service import CatchupRepService, LedgerCatchupComplete
from plenum.server.catchup.cons_proof_service import ConsProofService, ConsProofReady
from plenum.server.catchup.utils import CatchupDataProvider
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

        services_tx, services_rx = create_direct_channel()
        router = Router(services_rx)
        router.add(LedgerCatchupComplete, self._on_catchup_rep_service_stop)
        router.add(ConsProofReady, self._on_cons_proof_service_stop)

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

    def start(self, cons_proof: Optional[ConsistencyProof] = None):
        pass

    def _on_cons_proof_service_stop(self, msg: ConsProofReady):
        pass

    def _on_catchup_rep_service_stop(self, msg: LedgerCatchupComplete):
        pass
