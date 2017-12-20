from logging import getLogger

from plenum.common.constants import BATCH, OBSERVER_PREFIX
from plenum.common.messages.node_messages import ObservedData, BatchCommitted
from plenum.server.observer.observable_sync_policy import ObservableSyncPolicy
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType

logger = getLogger()


class ObservableSyncPolicyEachBatch(ObservableSyncPolicy):
    '''
    A simple policy propagating each committed batch to Observers
    '''

    def __init__(self, observable) -> None:
        super().__init__(observable)

    def can_process(self, observer_policy_type: ObserverSyncPolicyType):
        return observer_policy_type == ObserverSyncPolicyType.EACH_BATCH

    def process_new_batch(self, msg: BatchCommitted):
        if not self._observers:
            logger.debug("{} does not send BATCH to Observers since there are no observers added"
                         .format(OBSERVER_PREFIX))
            return
        msg = ObservedData(BATCH, msg)
        logger.debug("{} sending BATCH to Observers {}"
                     .format(OBSERVER_PREFIX, msg))
        self._observable.send_to_observers(msg, self._observers)
