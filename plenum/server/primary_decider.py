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

    def decidePrimaries(self) -> None:
        """
        Start election of the primary replica for each protocol instance        
        """
        raise NotImplementedError

    async def serviceQueues(self, limit):
        # TODO: this should be abstract
        raise NotImplementedError

    def viewChanged(self, viewNo: int):
        """
        Notifies primary decider about the fact that view changed to let it
        prepare for election, which then will be started from outside by 
        calling decidePrimaries() 
        """
        if viewNo <= self.viewNo:
            logger.warning("Provided view no {} is not greater than the "
                           "current view no {}".format(viewNo, self.viewNo))
            return False
        self.viewNo = viewNo
        self.previous_master_primary = self.node.master_primary_name
        for replica in self.replicas:
            replica.primaryName = None
        return True

    def get_msgs_for_lagged_nodes(self) -> List[object]:
        """
        Returns election messages from the last view change        
        """
        raise NotImplementedError

    def send(self, msg):
        """
        Send a message to the node on which this replica resides.

        :param msg: the message to send
        """
        logger.debug("{}'s elector sending {}".format(self.name, msg))
        self.outBox.append(msg)

    def start_election_for_instance(self, instance_id):
        """
        Called when starting election for a particular protocol instance 
        """
        raise NotImplementedError
