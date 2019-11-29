from typing import Optional

from plenum.common.config_util import getConfig
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.internal_messages import PrimarySelected, PrimaryDisconnected
from plenum.common.metrics_collector import MetricsCollector, NullMetricsCollector
from plenum.common.router import Subscription
from plenum.common.timer import TimerService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.utils import replica_name_to_node_name
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

        self._primary_disconnection_time = timer.get_current_time()  # type: Optional[float]

        self._subscription = Subscription()
        self._subscription.subscribe(network, ExternalBus.Connected, self.process_connected)
        self._subscription.subscribe(network, ExternalBus.Disconnected, self.process_disconnected)
        self._subscription.subscribe(bus, PrimarySelected, self.process_primary_selected)

    def cleanup(self):
        self._subscription.unsubscribe_all()

    def process_connected(self, msg: ExternalBus.Connected, frm: str):
        if frm == replica_name_to_node_name(self._data.primary_name):
            self._primary_disconnection_time = None

    def process_disconnected(self, msg: ExternalBus.Disconnected, frm: str):
        if frm == replica_name_to_node_name(self._data.primary_name):
            self._primary_disconnected()

    def process_primary_selected(self, msg: PrimarySelected):
        primary_name = replica_name_to_node_name(self._data.primary_name)
        primary_connected = self._data.is_primary or primary_name in self._network.connecteds
        if primary_connected:
            self._primary_disconnection_time = None
        else:
            self._primary_disconnected()

    def _primary_disconnected(self):
        self._primary_disconnection_time = self._timer.get_current_time()
        self._bus.send(PrimaryDisconnected(inst_id=self._data.inst_id))
