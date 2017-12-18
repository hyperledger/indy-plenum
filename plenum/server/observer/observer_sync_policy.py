from abc import ABCMeta, abstractmethod
from enum import Enum, unique

from plenum.common.messages.node_messages import ObservedData


@unique
class ObserverSyncPolicyType(Enum):
    EACH_BATCH = 0
    # TBD more


class ObserverSyncPolicy(metaclass=ABCMeta):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def apply_data(self, msg: ObservedData):
        pass
