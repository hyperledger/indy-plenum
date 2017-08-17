from abc import abstractmethod
from typing import Optional

from stp_core.common.log import getlogger

logger = getlogger()


class MessageFilter:

    @abstractmethod
    def filter_node_to_node(self, msg) -> Optional[str]:
        raise NotImplementedError

    @abstractmethod
    def filter_client_to_node(self, req) -> Optional[str]:
        raise NotImplementedError


class MessageFilterEngine:
    def __init__(self):
        self.__filters = {}

    def add_filter(self, name: str, filter: MessageFilter):
        self.__filters[name] = filter

    def remove_filter(self, name: str):
        self.__filters.pop(name, None)

    def filter_node_to_node(self, msg) -> Optional[str]:
        for fltr in self.__filters.values():
            filter_desc = fltr.filter_node_to_node(msg)
            if filter_desc:
                logger.debug(
                    "Filtered node-to-node msg {} since {}".format(msg, filter_desc))
                return filter_desc
        return None

    def filter_client_to_node(self, req) -> Optional[str]:
        for fltr in self.__filters.values():
            filter_desc = fltr.filter_client_to_node(req)
            if filter_desc:
                logger.debug(
                    "Filtered client request {} since {}".format(
                        req, filter_desc))
                return filter_desc
        return None
