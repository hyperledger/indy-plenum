from abc import ABC, abstractmethod

from plenum.common.timer import TimerService
from stp_core.common.log import getlogger

logger = getlogger()


# TODO docs and types
# TODO logging


class ViewChangerDataProvider(ABC):
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

    # PROPERTIES

    @property
    def view_no(self):
        return self.provider.view_no()

    @property
    def view_change_in_progress(self) -> bool:
        return self.provider.view_change_in_progress()
