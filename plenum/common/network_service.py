from abc import ABC, abstractmethod
from typing import List, Union, Any

from plenum.common.channel import RxChannel


class NetworkService(ABC):
    Destination = Union[None, str, List[str]]

    @abstractmethod
    def send(self, msg: Any, dst: Destination = None):
        """
        Send message to some or all peers

        :param msg: message to send
        :param dst: destination peer (or list of peers), None means broadcast to all known peers
        """
        pass

    @abstractmethod
    def on_message(self) -> RxChannel:
        """
        :return: channel with incoming messages, which can be subscribed to
        """
        pass
