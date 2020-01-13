from abc import ABC, abstractmethod

from plenum.common.timer import TimerService
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
    def view_change_in_progress(self) -> bool:
        return self.provider.view_change_in_progress()

    def on_master_degradation(self):
        self.propose_view_change()

    # TODO we have `on_primary_loss`, do we need that one?
    def on_primary_about_to_be_disconnected(self):
        self.propose_view_change(Suspicions.PRIMARY_ABOUT_TO_BE_DISCONNECTED)

    def on_suspicious_primary(self, suspicion: Suspicions):
        self.propose_view_change(suspicion)

    def on_node_count_changed(self):
        self.propose_view_change(Suspicions.NODE_COUNT_CHANGED)

    def propose_view_change(self, suspicion=Suspicions.PRIMARY_DEGRADED):
        self.provider.propose_view_change(suspicion)
