from plenum.common.constants import REPLY, BATCH
from plenum.common.messages.node_messages import ObservedData, BatchCommitted
from plenum.server.observer.observable_sync_policy import ObservableSyncPolicy
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType


class ObservableSyncPolicyEachBatch(ObservableSyncPolicy):
    def __init__(self, observable) -> None:
        super().__init__(observable)

    def can_process(self, observer_policy_type: ObserverSyncPolicyType):
        return observer_policy_type == ObserverSyncPolicyType.EACH_BATCH

    def process_new_batch(self, msg: BatchCommitted):
        msg = ObservedData(BATCH, msg)
        self._observable.send_to_observers(msg, self._observers)
