from abc import ABC, abstractmethod
from typing import List, Union, Any

from plenum.common.channel import RxChannel


class NetworkService(ABC):
    @abstractmethod
    def send(self, msg: Any, dst: Union[None, str, List[str]] = None):
        """
        Send message to some or all peers

        :param msg: message to send
        :param dst: destination peer (or list of peers), None means broadcast to all known peers
        """
        pass

    @abstractmethod
    def messages(self) -> RxChannel:
        """
        :return: channel with incoming messages, which can be subscribed to
        """
        pass
