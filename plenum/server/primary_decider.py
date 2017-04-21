from collections import deque

from plenum.common.message_processor import MessageProcessor
from plenum.server.has_action_queue import HasActionQueue


class PrimaryDecider(HasActionQueue, MessageProcessor):
    def __init__(self, node):
        HasActionQueue.__init__(self)

        self.name = node.name
        self.f = node.f
        self.replicas = node.replicas
        self.viewNo = node.viewNo
        self.rank = node.rank
        self.nodeNames = node.allNodeNames
        self.nodeCount = 0
        self.inBox = deque()
        self.outBox = deque()

    def decidePrimaries(self):
        """
        Choose the primary replica for each protocol instance in the system
        using a PrimaryDecider.
        """
        raise NotImplementedError

    async def serviceQueues(self, limit):
        return 0

