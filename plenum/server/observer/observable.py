from collections import deque

from plenum.common.message_processor import MessageProcessor
from plenum.common.messages.node_messages import ObservedData, BatchCommitted
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.observer.observable_sync_policy_each_batch import ObservableSyncPolicyEachBatch
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType
from plenum.server.router import Router


class Observable(HasActionQueue, MessageProcessor):
    '''
    Observable part of the Node needed to keep Observers in sync.
    It can have multiple strategies (policies) on how to notify Observers.
    As of now, just one simple policy is supported - Notify all Observers after each batch is committed.

    - Node sends BatchCommitted message to Observable (puts to inbox)
    - Observable creates ObservedData message depending on the strategy and puts to outbox
    - Node gets ObservedData message from outbox and sends to the specified Observers
    - As of now it's assumed that all Observers are nodes connected in the nodestack (as other Validators)
     '''

    def __init__(self) -> None:
        HasActionQueue.__init__(self)
        self._inbox = deque()
        self._outbox = deque()
        self._inbox_router = Router(
            (BatchCommitted, self.process_new_batch),
        )

        # TODO: support other policies
        self.__sync_policies = {
            ObserverSyncPolicyType.EACH_BATCH: ObservableSyncPolicyEachBatch(self)
        }

    # ADD/REMOVE OBSERVERS (delegated to Policies)

    def add_observer(self, observer_remote_id: str,
                     observer_policy_type: ObserverSyncPolicyType):
        for sync_policy in self.__sync_policies.values():
            sync_policy.add_observer(observer_remote_id, observer_policy_type)

    def remove_observer(self, observer_remote_id):
        for sync_policy in self.__sync_policies.values():
            sync_policy.remove_observer(observer_remote_id)

    def get_observers(self, observer_policy_type: ObserverSyncPolicyType):
        policy = self._get_policy(observer_policy_type)
        if not policy:
            return []
        return policy.get_observers()

    # MESSAGES from/to Node

    async def serviceQueues(self, limit=None) -> int:
        """
        Service at most `limit` messages from the inBox.

        :param limit: the maximum number of messages to service
        :return: the number of messages successfully processed
        """
        return await self._inbox_router.handleAll(self._inbox, limit)

    def append_input(self, msg):
        self._inbox.append(msg)

    def get_output(self):
        return self._outbox.popleft() if self._outbox else None

    # PROCESS BatchCommitted from Node Actor (delegated to Policies)

    def process_new_batch(self, msg: BatchCommitted):
        for sync_policy in self.__sync_policies.values():
            sync_policy.process_new_batch(msg)

    def send_to_observers(self, msg: ObservedData, observer_remote_ids):
        # As of now we assume that all Observers are nodes we can connect to
        # TODO: support other ways of connection to observers
        self._outbox.append((msg, observer_remote_ids))

    # PRIVATE

    def _get_policy(self, observer_policy_type: ObserverSyncPolicyType):
        if observer_policy_type not in self.__sync_policies:
            return None
        return self.__sync_policies[observer_policy_type]
