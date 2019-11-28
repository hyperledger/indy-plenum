from plenum.common.config_util import getConfig
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.metrics_collector import MetricsCollector, NullMetricsCollector
from plenum.common.router import Subscription
from plenum.common.timer import TimerService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from stp_core.common.log import getlogger

logger = getlogger()


class PrimaryConnectionMonitorService:
    def __init__(self,
                 data: ConsensusSharedData,
                 timer: TimerService,
                 bus: InternalBus,
                 network: ExternalBus,
                 metrics: MetricsCollector = NullMetricsCollector()):
        self._data = data
        self._timer = timer
        self._bus = bus
        self._network = network
        self.metrics = metrics

        self._config = getConfig()

        self._subscription = Subscription()
        self._subscription.subscribe(network, ExternalBus.Connected(), self.process_connected)
        self._subscription.subscribe(network, ExternalBus.Disconnected(), self.process_disconnected)

    def cleanup(self):
        self._subscription.unsubscribe_all()

    def process_connected(self, msg: ExternalBus.Connected, frm: str):
        pass

    def process_disconnected(self, msg: ExternalBus.Disconnected, frm: str):
        pass
