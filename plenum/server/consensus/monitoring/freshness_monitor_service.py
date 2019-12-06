from plenum.common.config_util import getConfig
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.metrics_collector import MetricsCollector, NullMetricsCollector
from plenum.common.timer import TimerService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from stp_core.common.log import getlogger

logger = getlogger()


class FreshnessMonitorService:
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

    def cleanup(self):
        pass
