from collections import deque
from logging import getLogger

from plenum.common.message_processor import MessageProcessor
from plenum.common.messages.node_messages import ObservedData
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.observer.observer import Observer
from plenum.server.observer.observer_sync_policy_each_batch import ObserverSyncPolicyEachBatch
from plenum.server.router import Router

logger = getLogger()


class NodeObserver(Observer, HasActionQueue, MessageProcessor):
    '''
    An observer attached to each Node.
    As of now, it supports the simplest policy when each committed batch by Validators
    is propagated and applied to Observers.
    '''

    def __init__(self, node) -> None:
        self._node = node
        super().__init__(
            [ObserverSyncPolicyEachBatch(self._node)]
        )
        HasActionQueue.__init__(self)

        self._inbox = deque()
        self._inbox_router = Router(
            (ObservedData, self.apply_data),
        )

    @property
    def observer_remote_id(self) -> str:
        return self._node.name

    # MESSAGES from/to Node

    async def serviceQueues(self, limit=None) -> int:
        """
        Service at most `limit` messages from the inBox.

        :param limit: the maximum number of messages to service
        :return: the number of messages successfully processed
        """
        return await self._inbox_router.handleAll(self._inbox, limit)

    def append_input(self, msg, sender):
        self._inbox.append((msg, sender))
