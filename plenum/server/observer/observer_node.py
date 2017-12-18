from logging import getLogger

from plenum.server.observer.observer import Observer
from plenum.server.observer.observer_sync_policy_each_batch import ObserverSyncPolicyEachBatch

logger = getLogger()


class NodeObserver(Observer):
    def __init__(self, node) -> None:
        self._node = node
        super().__init__(
            [ObserverSyncPolicyEachBatch(self._node)]
        )

    @property
    def observer_remote_id(self) -> str:
        return self._node.name
