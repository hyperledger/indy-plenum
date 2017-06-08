from typing import Iterable, List

from plenum.common.types import ViewChangeDone
from plenum.server.router import Route
from stp_core.common.log import getlogger
from plenum.server.primary_decider import PrimaryDecider
from plenum.server.replica import Replica
from plenum.common.util import mostCommonElement, get_strong_quorum

logger = getlogger()


class PrimarySelector(PrimaryDecider):
    """
    Simple implementation of primary decider. 
    Decides on a primary in round-robin fashion.
    Assumes that all nodes are up
    """

    def __init__(self, node):
        super().__init__(node)
        self.previous_master_primary = None
        self._view_change_done = {}  # instance id -> replica name -> data
        self._ledger_info = None  # ledger info for view change

    @property
    def routes(self) -> Iterable[Route]:
        return [(ViewChangeDone, self._processViewChangeDoneMessage)]

    # overridden method of PrimaryDecider
    async def serviceQueues(self, limit=None):
        raise NotImplementedError

    # overridden method of PrimaryDecider
    def get_msgs_for_lagged_nodes(self) -> List[ViewChangeDone]:
        """
        Returns the last `ViewChangeDone` message sent for specific instance.
        If no view change has happened returns ViewChangeDone
        with view no 0 to a newly joined node 
        """
        messages = []
        for instance_id, replica_messages in self._view_change_done.items():
            for message in replica_messages.values():
                (new_primary_replica_name, ledger_info) = message
                messages.append(ViewChangeDone(new_primary_replica_name,
                                               instance_id,
                                               self.viewNo,
                                               ledger_info))
        return messages

    # overridden method of PrimaryDecider
    def start_election_for_instance(self, instance_id):
        # TODO: this should probably be removed, together with super def
        logger.warning("Starting election for specific instance is not "
                       "supported, starting view change for all instead")
        self.decidePrimaries()

    # overridden method of PrimaryDecider
    def decidePrimaries(self):
        self._startSelection()

    def _is_master_instance(self, instance_id):
        # TODO: get master instance from outside
        # Instance 0 is always master
        return instance_id == 0

    def _processViewChangeDoneMessage(self,
                                      msg: ViewChangeDone,
                                      sender: str) -> None:
        """
        Processes ViewChangeDone messages. Once 2f + 1 messages have been 
        received, decides on a primary for specific replica. 

        :param msg: ViewChangeDone message
        :param sender: the name of the node from which this message was sent
        """

        logger.debug("{}'s primary selector started processing of "
                     "ViewChangeDone msg from {} : {}"
                     .format(self.name, sender, msg))

        proposed_view_no = msg.viewNo

        if self.viewNo > proposed_view_no:
            self.discard(msg,
                         '{} got Primary from {} for view no {} '
                         'whereas current view no is {}'
                         .format(self, sender, proposed_view_no, self.viewNo),
                         logMethod=logger.warning)
            return

        new_primary_replica_name = msg.name
        instance_id = msg.instId
        ledger_info = msg.ledgerInfo
        sender_replica_name = Replica.generateName(sender, instance_id)
        new_primary_node_name = Replica.getNodeName(new_primary_replica_name)

        if self._is_master_instance(instance_id) and \
           new_primary_node_name == self.previous_master_primary:
            self.discard(msg,
                         '{} got Primary from {} for {} who was primary of '
                         'master in previous view too'
                         .format(self, sender, new_primary_replica_name),
                         logMethod=logger.warning)
            return

        if not self._mark_replica_as_changed_view(instance_id,
                                                  sender_replica_name,
                                                  new_primary_node_name,
                                                  ledger_info):
            self.discard(msg,
                         "already marked {} as done view change".
                         format(sender_replica_name),
                         logger.warning)
            return

        replica = self.replicas[instance_id]  # type: Replica

        if replica.hasPrimary:
            self.discard(msg,
                         "it already decided primary which is {}".
                         format(replica.primaryName),
                         logger.debug)
            return

        if not self._hasViewChangeQuorum(instance_id):
            logger.debug("{} received ViewChangeDone from {}, "
                         "but have got no quorum yet"
                         .format(self.name, sender))
            return

        self._complete_primary_selection(instance_id, replica)

    def _complete_primary_selection(self, instance_id, replica):
        """
        This method is called when sufficient number of ViewChangeDone
        received and makes steps to switch to the new primary
        """

        # TODO: implement case when we get equal number of ViewChangeDone
        # with different primaries specified

        votes = self._view_change_done[instance_id].values()
        new_primary, ledger_info = mostCommonElement(votes)

        expected_primary = self._who_is_the_next_primary(instance_id)
        if new_primary != expected_primary:
            logger.warning("{} expected next primary to be {}, but majority "
                           "declared {} instead"
                           .format(self.name, expected_primary, new_primary))
        # TODO: check if ledger status is expected

        logger.display("{} declares view change {} as completed for "
                       "instance {}, "
                       "new primary is {}, "
                       "ledger info is {}"
                       .format(replica,
                               self.viewNo,
                               instance_id,
                               new_primary,
                               ledger_info),
                       extra={"cli": "ANNOUNCE",
                              "tags": ["node-election"]})

        replica.primaryChanged(new_primary)

        if self._is_master_instance(instance_id):
            self.previous_master_primary = None
            self.node.primary_found()

    def _mark_replica_as_changed_view(self,
                                      instance_id,
                                      replica_name,
                                      new_primary_replica_name,
                                      ledger_info):
        if instance_id not in self._view_change_done:
            self._view_change_done[instance_id] = {}
        if replica_name in self._view_change_done:
            return False
        data = (new_primary_replica_name, ledger_info)
        self._view_change_done[instance_id][replica_name] = data
        return True

    def _hasViewChangeQuorum(self, instance_id):
        """
        Checks whether 2f+1 nodes completed view change and whether one 
        of them is the next primary
        """
        declarations = self._view_change_done.get(instance_id, [])
        num_of_ready_nodes = len(declarations)
        quorum = get_strong_quorum(f=self.f)
        next_primary_name = self._who_is_the_next_primary(instance_id)
        enough_nodes = num_of_ready_nodes >= quorum

        if not enough_nodes:
            return False

        if next_primary_name not in declarations:
            logger.trace("{} got enough ViewChangeDone messages "
                         "for quorum ({}), but the next primary {} "
                         "has not answered yet"
                         .format(self.name,
                                 num_of_ready_nodes,
                                 quorum,
                                 instance_id))
            return False

        logger.trace("{} got view change quorum ({} >= {}) for instance {}"
                     .format(self.name,
                             num_of_ready_nodes,
                             quorum,
                             instance_id))
        return True

    def _startSelection(self):
        logger.debug("{} starting selection".format(self))
        for instance_id, replica in enumerate(self.replicas):
            if replica.primaryName is not None:
                logger.debug('{} already has a primary'.format(replica))
                continue
            new_primary_name = self._who_is_the_next_primary(instance_id)
            logger.display("{} selected primary {} for instance {} (view {})"
                           .format(replica,
                                   new_primary_name,
                                   instance_id,
                                   self.viewNo),
                           extra={"cli": "ANNOUNCE",
                                  "tags": ["node-election"]})
            self._send_view_change_done_message(instance_id)


    def _who_is_the_next_primary(self, instance_id):
        """
        Returnes name of the next node which is supposed to be a new Primary
        in round-robin fashion
        """
        new_primary_id = (self.viewNo + instance_id) % self.node.totalNodes
        new_primary_name = Replica.generateName(
            nodeName=self.node.get_name_by_rank(new_primary_id),
            instId=instance_id)
        return new_primary_name

    def _send_view_change_done_message(self, instance_id):
        """
        Sends ViewChangeDone message to other protocol participants
        """

        next_primary = self._who_is_the_next_primary(instance_id)
        next_viewno = self.viewNo + 1
        ledger_info = []
        message = ViewChangeDone(next_primary,
                                 instance_id,
                                 next_viewno,
                                 ledger_info)
        self.send(message)
