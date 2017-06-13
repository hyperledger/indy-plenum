from abc import abstractmethod
from typing import Optional

from stp_core.common.log import getlogger

logger = getlogger()

class MessageFilter:

    @abstractmethod
    def filter(self, msg) -> Optional[str]:
        raise NotImplementedError


class MessageFilterEngine:

    def __init__(self):
        self.__filters = {}

    def add_filter(self, name: str, filter: MessageFilter):
        self.__filters[name] = filter

    def remove_filter(self, name: str):
        del self.__filters[name]

    def filter(self, msg):
        for fltr in self.__filters.values():
            filter_desc = fltr.filter(msg)
            if filter_desc:
                logger.debug("Filtered msg {} since {}", msg, filter_desc)
                return False
        return True