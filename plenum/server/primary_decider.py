from typing import Iterable, Optional
from collections import deque

from plenum.common.constants import VIEW_CHANGE_PREFIX
from plenum.common.message_processor import MessageProcessor
from plenum.common.types import f
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.router import Router, Route
from stp_core.common.log import getlogger
from typing import List
from abc import ABCMeta, abstractmethod

logger = getlogger()


class PrimaryDecider(HasActionQueue, MessageProcessor, metaclass=ABCMeta):

    def __init__(self, node):
        HasActionQueue.__init__(self)
        self.node = node

        self.name = node.name
        self.replicas = node.replicas
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
    def viewNo(self) -> Optional[int]:
        return self.node.viewNo

    @viewNo.setter
    def viewNo(self, value: int):
        self.node.viewNo = value

    @property
    def rank(self) -> Optional[int]:
        return self.node.rank

    @property
    def was_master_primary_in_prev_view(self):
        return self.previous_master_primary == self.name

    @property
    def master_replica(self):
        return self.node.master_replica

    @property
    @abstractmethod
    def routes(self) -> Iterable[Route]:
        pass

    @property
    def supported_msg_types(self) -> Iterable[type]:
        return [k for k, v in self.routes]

    @abstractmethod
    def decidePrimaries(self) -> None:
        """
        Start election of the primary replica for each protocol instance
        """

    def filterMsgs(self, wrappedMsgs: deque) -> deque:
        """
        Filters messages by view number so that only the messages that have the
        current view number are retained.

        :param wrappedMsgs: the messages to filter
        """
        filtered = deque()
        while wrappedMsgs:
            wrappedMsg = wrappedMsgs.popleft()
            msg, sender = wrappedMsg
            if hasattr(msg, f.VIEW_NO.nm):
                reqViewNo = getattr(msg, f.VIEW_NO.nm)
                if reqViewNo == self.viewNo:
                    filtered.append(wrappedMsg)
                else:
                    self.discard(wrappedMsg,
                                 "its view no {} is less than the elector's {}"
                                 .format(reqViewNo, self.viewNo),
                                 logger.debug)
            else:
                filtered.append(wrappedMsg)

        return filtered

    async def serviceQueues(self, limit=None) -> int:
        """
        Service at most `limit` messages from the inBox.

        :param limit: the maximum number of messages to service
        :return: the number of messages successfully processed
        """

        return await self.inBoxRouter.handleAll(self.filterMsgs(self.inBox),
                                                limit)

    def view_change_started(self, viewNo: int):
        """
        Notifies primary decider about the fact that view changed to let it
        prepare for election, which then will be started from outside by
        calling decidePrimaries()
        """
        if viewNo <= self.viewNo:
            logger.warning("{}Provided view no {} is not greater"
                           " than the current view no {}"
                           .format(VIEW_CHANGE_PREFIX, viewNo, self.viewNo))
            return False
        self.previous_master_primary = self.node.master_primary_name
        for replica in self.replicas.values():
            replica.primaryName = None
        return True

    @abstractmethod
    def get_msgs_for_lagged_nodes(self) -> List[object]:
        """
        Returns election messages from the last view change
        """

    def send(self, msg):
        """
        Send a message to the node on which this replica resides.

        :param msg: the message to send
        """
        logger.debug("{}'s elector sending {}".format(self.name, msg))
        self.outBox.append(msg)

    @abstractmethod
    def start_election_for_instance(self, instance_id):
        """
        Called when starting election for a particular protocol instance
        """

    @abstractmethod
    def on_catchup_complete(self):
        """
        Select primaries after catchup completed
        """

    @abstractmethod
    def process_selection(self, instance_count, node_reg, node_ids):
        """
        Return primaries set for current view_no
        """
