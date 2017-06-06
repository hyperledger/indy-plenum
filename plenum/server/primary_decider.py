from typing import Iterable
from collections import deque

from plenum.common.message_processor import MessageProcessor
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.router import Router, Route
from stp_core.common.log import getlogger
from typing import List

logger = getlogger()


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

        # Need to keep track of who was primary for the master protocol
        # instance for previous view, this variable only matters between
        # elections, the elector will set it before doing triggering new
        # election and will reset it after primary is decided for master
        # instance
        self.previous_master_primary = None

    def __repr__(self):
        return "{}".format(self.name)

    @property
    def was_master_primary_in_prev_view(self):
        return self.previous_master_primary == self.name

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
        if viewNo > self.viewNo:
            self.viewNo = viewNo
            self.previous_master_primary = self.node.master_primary
            for replica in self.replicas:
                replica.primaryName = None
            return True
        else:
            logger.warning("Provided view no {} is not greater than the "
                           "current view no {}".format(viewNo, self.viewNo))
            return False

    def get_msgs_for_lagged_nodes(self) -> List[int]:
        raise NotImplementedError
