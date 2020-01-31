from typing import Optional

from plenum.common.config_util import getConfig
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.internal_messages import PrimarySelected, PrimaryDisconnected, VoteForViewChange, \
    NodeStatusUpdated
from plenum.common.metrics_collector import MetricsCollector, NullMetricsCollector
from plenum.common.router import Subscription
from plenum.common.startable import Status
from plenum.common.timer import TimerService, RepeatingTimer
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.server.suspicion_codes import Suspicions
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

        self._propose_view_change_timer = RepeatingTimer(timer=timer,
                                                         interval=self._config.NEW_VIEW_TIMEOUT,
                                                         callback=self._propose_view_change_if_needed,
                                                         active=False)

        self._subscription = Subscription()
        self._subscription.subscribe(network, ExternalBus.Connected, self.process_connected)
        self._subscription.subscribe(network, ExternalBus.Disconnected, self.process_disconnected)
        self._subscription.subscribe(bus, PrimarySelected, self.process_primary_selected)
        self._subscription.subscribe(bus, NodeStatusUpdated, self.process_node_status_updated)

        if self._data.is_master:
            self._schedule_primary_connection_check(delay=self._config.INITIAL_PROPOSE_VIEW_CHANGE_TIMEOUT)

    def __str__(self):
        return self._data.name

    def cleanup(self):
        self._subscription.unsubscribe_all()
        self._timer.cancel(self._check_primary_connection)
        self._propose_view_change_timer.stop()

    def process_connected(self, msg: ExternalBus.Connected, frm: str):
        if frm == replica_name_to_node_name(self._data.primary_name):
            self._primary_connected()

    def process_disconnected(self, msg: ExternalBus.Disconnected, frm: str):
        if frm == replica_name_to_node_name(self._data.primary_name):
            self._primary_disconnected()

    def process_primary_selected(self, msg: PrimarySelected):
        if self._is_primary_connected():
            self._primary_connected()
        else:
            self._primary_disconnected()

    def process_node_status_updated(self, msg: NodeStatusUpdated):
        # TODO: This was ported from old code, but there is quite high chance
        #  that this functionality is not needed anymore
        if msg.old_status == Status.starting \
                and msg.new_status == Status.started_hungry \
                and self._primary_disconnection_time is not None \
                and self._data.primary_name is not None:
            """
            Such situation may occur if the pool has come back to reachable consensus but
            primary is still disconnected, so view change proposal makes sense now.
            """
            self._schedule_primary_connection_check()

    def _primary_connected(self):
        self._primary_disconnection_time = None

        if self._data.is_master:
            logger.display('{} restored connection to primary of master'.format(self))
            self._timer.cancel(self._check_primary_connection)
            self._propose_view_change_timer.stop()

    def _primary_disconnected(self):
        self._primary_disconnection_time = self._timer.get_current_time()
        self._bus.send(PrimaryDisconnected(inst_id=self._data.inst_id))

        if self._data.is_master:
            logger.display('{} lost connection to primary of master'.format(self))
            self._schedule_primary_connection_check()

    def _schedule_primary_connection_check(self, delay: Optional[float] = None):
        if delay is None:
            delay = self._config.ToleratePrimaryDisconnection

        logger.info('{} scheduling primary connection check in {} sec'.format(self, delay))
        self._timer.schedule(delay=delay, callback=self._check_primary_connection)

    def _check_primary_connection(self):
        if self._primary_disconnection_time is None:
            logger.info('{} The primary is already connected '
                        'so view change will not be proposed'.format(self))
            return

        logger.display("{} primary has been disconnected for too long".format(self))

        if not self._data.is_ready or not self._data.is_synced:
            logger.info('{} The node is not ready yet so '
                        'view change will not be proposed now, but re-scheduled.'.format(self))
            self._schedule_primary_connection_check()
            return

        self._propose_view_change()
        self._propose_view_change_timer.start()

    def _is_primary_connected(self):
        if self._data.is_primary:
            return True

        primary_name = replica_name_to_node_name(self._data.primary_name)
        if primary_name in self._network.connecteds:
            return True

        return primary_name not in self._data.validators

    def _propose_view_change(self):
        self._bus.send(VoteForViewChange(suspicion=Suspicions.PRIMARY_DISCONNECTED))

    def _propose_view_change_if_needed(self):
        if self._primary_disconnection_time is not None:
            self._propose_view_change()
