from plenum.common.config_util import getConfig
from plenum.common.event_bus import InternalBus
from plenum.common.messages.internal_messages import VoteForViewChange
from plenum.common.timer import TimerService, RepeatingTimer
from plenum.server.suspicion_codes import Suspicions
from stp_core.common.log import getlogger

logger = getlogger()


class ForcedViewChangeService:
    def __init__(self,
                 timer: TimerService,
                 bus: InternalBus):
        self._timer = timer
        self._bus = bus

        self._config = getConfig()

        # Force periodic view change if enabled in config
        force_view_change_freq = self._config.ForceViewChangeFreq
        if force_view_change_freq > 0:
            self._force_view_change_timer = RepeatingTimer(self._timer, force_view_change_freq, self._force_view_change)

    def cleanup(self):
        self._force_view_change_timer.stop()

    def _force_view_change(self):
        self._bus.send(VoteForViewChange(Suspicions.DEBUG_FORCE_VIEW_CHANGE))
