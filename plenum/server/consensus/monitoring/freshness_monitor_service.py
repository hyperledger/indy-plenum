from typing import Optional, Callable

from plenum.common.config_util import getConfig
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.internal_messages import VoteForViewChange
from plenum.common.metrics_collector import MetricsCollector, NullMetricsCollector
from plenum.common.timer import TimerService, RepeatingTimer
from plenum.common.util import get_utc_epoch
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.replica_freshness_checker import FreshnessChecker
from plenum.server.suspicion_codes import Suspicions
from stp_core.common.log import getlogger

logger = getlogger()


class FreshnessMonitorService:
    def __init__(self,
                 data: ConsensusSharedData,
                 timer: TimerService,
                 bus: InternalBus,
                 network: ExternalBus,
                 freshness_checker: FreshnessChecker,
                 get_time_for_3pc_batch: Optional[Callable[[], int]] = None,
                 metrics: MetricsCollector = NullMetricsCollector()):
        self._data = data
        self._timer = timer
        self._bus = bus
        self._network = network
        self.metrics = metrics

        self._freshness_checker = freshness_checker
        self._get_time_for_3pc_batch = get_time_for_3pc_batch if get_time_for_3pc_batch is not None else get_utc_epoch

        self._config = getConfig()

        # Start periodic freshness check
        state_freshness_update_interval = self._config.STATE_FRESHNESS_UPDATE_INTERVAL
        if state_freshness_update_interval > 0:
            self._check_freshness_timer = RepeatingTimer(self._timer, state_freshness_update_interval, self._check_freshness)

    def cleanup(self):
        self._check_freshness_timer.stop()

    def _check_freshness(self):
        if self._is_state_fresh_enough():
            logger.debug("{} not sending instance change because found state to be fresh enough".format(self))
            return
        self._bus.send(VoteForViewChange(Suspicions.STATE_SIGS_ARE_NOT_UPDATED))

    def _is_state_fresh_enough(self) -> bool:
        # Don't trigger view change while catching up
        # TODO: INDY-2324 Do we really need to check waiting_for_new_view as well now?
        if not self._data.is_synced and not self._data.waiting_for_new_view:
            return True

        threshold = self._config.ACCEPTABLE_FRESHNESS_INTERVALS_COUNT * self._config.STATE_FRESHNESS_UPDATE_INTERVAL
        return self._state_freshness() < threshold

    def _state_freshness(self) -> float:
        timestamps_per_ledger = self._get_ledgers_last_update_time().values()
        return self._get_time_for_3pc_batch() - min(timestamps_per_ledger)

    def _get_ledgers_last_update_time(self) -> dict:
        return self._freshness_checker.get_last_update_time()
