from abc import ABC, abstractmethod

from plenum.common.startable import Mode
from plenum.common.timer import TimerService, RepeatingTimer
from plenum.server.quorums import Quorums
from stp_core.common.log import getlogger

from plenum.server.suspicion_codes import Suspicions, Suspicion

logger = getlogger()


# TODO docs and types
# TODO logging


class ViewChangerDataProvider(ABC):
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def config(self) -> object:
        pass

    @abstractmethod
    def quorums(self) -> Quorums:
        pass

    @abstractmethod
    def node_mode(self) -> Mode:
        pass

    @abstractmethod
    def is_primary_disconnected(self) -> bool:
        pass

    @abstractmethod
    def state_freshness(self) -> float:
        pass

    @abstractmethod
    def propose_view_change(self, suspicion: Suspicion):
        pass

    @abstractmethod
    def view_no(self):
        pass

    @abstractmethod
    def view_change_in_progress(self):
        pass


class ViewChanger():

    def __init__(self, provider: ViewChangerDataProvider, timer: TimerService):
        self.provider = provider
        self._timer = timer

        self.previous_view_no = None

        # Force periodic view change if enabled in config
        force_view_change_freq = self.config.ForceViewChangeFreq
        if force_view_change_freq > 0:
            RepeatingTimer(self._timer, force_view_change_freq, self.on_master_degradation)

        # Start periodic freshness check
        state_freshness_update_interval = self.config.STATE_FRESHNESS_UPDATE_INTERVAL
        if state_freshness_update_interval > 0:
            RepeatingTimer(self._timer, state_freshness_update_interval, self.check_freshness)

    def __repr__(self):
        return "{}".format(self.name)

    # PROPERTIES

    @property
    def view_no(self):
        return self.provider.view_no()

    @property
    def name(self) -> str:
        return self.provider.name()

    @property
    def config(self) -> object:
        return self.provider.config()

    @property
    def quorums(self) -> Quorums:
        return self.provider.quorums()

    @property
    def view_change_in_progress(self) -> bool:
        return self.provider.view_change_in_progress()

    @property
    def quorum(self) -> int:
        return self.quorums.view_change_done.value

    def on_master_degradation(self):
        self.propose_view_change()

    def check_freshness(self):
        if self.is_state_fresh_enough():
            logger.debug("{} not sending instance change because found state to be fresh enough".format(self))
            return
        self.propose_view_change(Suspicions.STATE_SIGS_ARE_NOT_UPDATED)

    # TODO we have `on_primary_loss`, do we need that one?
    def on_primary_about_to_be_disconnected(self):
        self.propose_view_change(Suspicions.PRIMARY_ABOUT_TO_BE_DISCONNECTED)

    def on_suspicious_primary(self, suspicion: Suspicions):
        self.propose_view_change(suspicion)

    def on_node_count_changed(self):
        self.propose_view_change(Suspicions.NODE_COUNT_CHANGED)

    def propose_view_change(self, suspicion=Suspicions.PRIMARY_DEGRADED):
        self.provider.propose_view_change(suspicion)

    def is_state_fresh_enough(self):
        threshold = self.config.ACCEPTABLE_FRESHNESS_INTERVALS_COUNT * self.config.STATE_FRESHNESS_UPDATE_INTERVAL
        return self.provider.state_freshness() < threshold or (not self.view_change_in_progress and
                                                               not Mode.is_done_syncing(self.provider.node_mode()))
