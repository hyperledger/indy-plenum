from abc import ABCMeta, abstractmethod

from plenum.common.messages.node_messages import BatchCommitted
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType


class ObservableSyncPolicy(metaclass=ABCMeta):
    def __init__(self, observable) -> None:
        self._observable = observable
        self._observers = []

    def add_observer(self,
                     observer_remote_id: str,
                     observer_policy_type: ObserverSyncPolicyType):
        if not self.can_process(observer_policy_type):
            return
        self._observers.append(observer_remote_id)

    def remove_observer(self, observer_remote_id):
        try:
            self._observers.remove(observer_remote_id)
        except ValueError:
            pass

    def get_observers(self):
        return self._observers

    @abstractmethod
    def can_process(self, observer_policy_type: ObserverSyncPolicyType):
        pass

    @abstractmethod
    def process_new_batch(self, msg: BatchCommitted):
        pass
