from typing import Iterable
from collections import deque

from plenum.common.message_processor import MessageProcessor
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.router import Router, Route


class PrimaryDecider(HasActionQueue, MessageProcessor):
    def __init__(self, node):
        HasActionQueue.__init__(self)
        # TODO: How does primary decider ensure that a node does not have a
        # primary while its catching up
        self.node = node

        self.name = node.name
        self.f = node.f
        self.replicas = node.replicas
        self.viewNo = node.viewNo
        self.rank = node.rank
        self.nodeNames = node.allNodeNames
        self.nodeCount = 0
        self.inBox = deque()
        self.outBox = deque()
        self.inBoxRouter = Router(*self.routes)

    def __repr__(self):
        return "{}".format(self.name)

    @property
    def routes(self) -> Iterable[Route]:
        raise NotImplementedError

    @property
    def supported_msg_types(self) -> Iterable[type]:
        return [k for k, v in self.routes]

    def decidePrimaries(self):
        """
        Choose the primary replica for each protocol instance in the system
        using a PrimaryDecider.
        """
        raise NotImplementedError

    async def serviceQueues(self, limit):
        return 0

    def viewChanged(self, viewNo: int):
        raise NotImplementedError
