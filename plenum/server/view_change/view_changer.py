import logging
from abc import ABC, abstractmethod
from typing import Optional, Set
from functools import partial

from plenum.common.startable import Mode
from plenum.common.timer import TimerService, RepeatingTimer
from plenum.server.quorums import Quorums
from storage.kv_store import KeyValueStorage
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

        # Action for _schedule instanceChange messages
        self.instance_change_action = None

        # Count of instance change rounds
        self.instance_change_rounds = 0

        # Time for view_change_starting
        self.start_view_change_ts = 0

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

    def _propose_view_change_if_needed(self, proposed_view_no, suspicion):
        # if scheduled action will be awakened after view change completed,
        # then this action must be stopped also.
        if self.view_no < proposed_view_no and self.provider.is_primary_disconnected():
            # Resend the same instance change message if we are not archive
            # InstanceChange quorum
            logger.info("Resend instance change message to all recipients")
            self.propose_view_change(suspicion)
            self._timer.schedule(self.config.NEW_VIEW_TIMEOUT,
                                 self.instance_change_action)
            logger.info("Count of rounds without quorum of "
                        "instance change messages: {}".format(self.instance_change_rounds))
            self.instance_change_rounds += 1
        else:
            # ViewChange procedure was started, therefore stop scheduling
            # resending instanceChange messages
            logger.info("Stop scheduling instance change resending")
            self._timer.cancel(self.instance_change_action)
            self.instance_change_action = None
            self.instance_change_rounds = 0

    def on_primary_loss(self):
        self.propose_view_change(Suspicions.PRIMARY_DISCONNECTED)
        proposed_view_no = self.view_no
        if not self.view_change_in_progress:
            proposed_view_no += 1
        if self.instance_change_action:
            # It's an action, scheduled for previous instanceChange round
            logger.info("Stop previous instance change resending schedule")
            self._timer.cancel(self.instance_change_action)
            self.instance_change_rounds = 0
        self.instance_change_action = partial(self._propose_view_change_if_needed,
                                              proposed_view_no,
                                              Suspicions.PRIMARY_DISCONNECTED)
        self._timer.schedule(self.config.NEW_VIEW_TIMEOUT,
                             self.instance_change_action)

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
