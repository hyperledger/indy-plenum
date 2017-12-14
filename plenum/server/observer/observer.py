from abc import abstractmethod, ABCMeta

from plenum.common.messages.node_messages import ObservedData


class Observer(metaclass=ABCMeta):
    @abstractmethod
    def apply_data(self, msg: ObservedData):
        pass

    @abstractmethod
    @property
    def observer_remote_id(self) -> str:
        pass
