from abc import ABCMeta, abstractmethod
from enum import Enum, unique


@unique
class ObserverSyncPolicyType(Enum):
    EACH_BATCH = 0
    # TBD more


class ObserverSyncPolicy(metaclass=ABCMeta):
    '''
    Abstract policy defining how to process incoming ObservedData messages
    (in fact, the policies should match ObservedData msg type and ObservableSyncPolicies)
    '''

    def __init__(self) -> None:
        pass

    @abstractmethod
    def apply_data(self, msg):
        pass

    @property
    @abstractmethod
    def policy_type(self) -> str:
        pass
