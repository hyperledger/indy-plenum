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
        self._ledger_manager = self.node.ledgerManager

    @property
    def routes(self) -> Iterable[Route]:
        return [(ViewChangeDone, self._processViewChangeDoneMessage)]

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
                                               instance_id, self.viewNo,
                                               ledger_info))
        return messages

    # overridden method of PrimaryDecider
    def decidePrimaries(self):
        self._startSelection()

    # Question: Master is always 0, until we change that rule why incur cost
    # of a method call, also name is confusing
    def _is_master_instance(self, instance_id):
        # TODO: get master instance from outside
        # Instance 0 is always master
        return instance_id == 0

    def _processViewChangeDoneMessage(self,
                                      msg: ViewChangeDone,
                                      sender: str) -> bool:
        """
        Processes ViewChangeDone messages. Once 2f + 1 messages have been 
        received, decides on a primary for specific replica. 

        :param msg: ViewChangeDone message
        :param sender: the name of the node from which this message was sent
        """

        logger.debug("{}'s primary selector started processing of "
                     "ViewChangeDone msg from {} : {}"
                     .format(self.name, sender, msg))

        view_no = msg.viewNo

        if self.viewNo != view_no:
            self.discard(msg,
                         '{} got Primary from {} for view no {} '
                         'whereas current view no is {}'
                         .format(self, sender, view_no, self.viewNo),
                         logMethod=logger.warning)
            return False

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
            return False

        if not self._mark_replica_as_changed_view(instance_id,
                                                  sender_replica_name,
                                                  new_primary_replica_name,
                                                  ledger_info):
            self.discard(msg,
                         "already marked {} as done view change".
                         format(sender_replica_name),
                         logger.warning)
            return False

        replica = self.replicas[instance_id]  # type: Replica

        if replica.hasPrimary:
            self.discard(msg,
                         "it already decided primary which is {}".
                         format(replica.primaryName),
                         logger.debug)
            return False

        # TODO: Result of `_hasViewChangeQuorum` should be cached
        if not self._hasViewChangeQuorum(instance_id):
            logger.debug("{} received ViewChangeDone from {}, "
                         "but have got no quorum yet"
                         .format(self.name, sender))
            return False

        rv = self.has_sufficient_same_view_change_done_messages(instance_id)
        if rv is None:
            return False

        self._complete_primary_selection(instance_id, replica, *rv)

    def has_sufficient_same_view_change_done_messages(self, instance_id):
        # TODO: Does not look like optimal implementation.
        votes = self._view_change_done[instance_id].values()
        votes = [(nm, tuple(tuple(i) for i in info)) for nm, info in votes]
        new_primary, ledger_info = mostCommonElement(votes)
        if votes.count((new_primary, ledger_info)) >= get_strong_quorum(self.f):
            logger.debug('{} found acceptable primary {} and ledger info {}'.
                         format(self, new_primary, ledger_info))
            return new_primary, ledger_info
        else:
            logger.debug('{} does not have acceptable primary'.format(self))
            return None

    def _complete_primary_selection(self, instance_id, replica, new_primary,
                                    ledger_info):
        """
        This method is called when sufficient number of ViewChangeDone
        received and makes steps to switch to the new primary
        """

        # TODO: implement case when we get equal number of ViewChangeDone
        # with different primaries specified

        expected_primary = self._who_is_the_next_primary(instance_id)
        if new_primary != expected_primary:
            logger.error("{} expected next primary to be {}, but majority "
                           "declared {} instead for view {}"
                           .format(self.name, expected_primary, new_primary,
                                   self.viewNo))
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
        # This method should just be present for master instance.
        """
        Checks whether 2f+1 nodes completed view change and whether one 
        of them is the next primary
        """
        declarations = self._view_change_done.get(instance_id, {})
        num_of_ready_nodes = len(declarations)
        quorum = get_strong_quorum(f=self.f)
        enough_nodes = num_of_ready_nodes >= quorum
        if not enough_nodes:
            return False

        next_primary_name = self._who_is_the_next_primary(instance_id)
        if next_primary_name not in declarations:
            logger.trace("{} got enough ViewChangeDone messages "
                         "for quorum ({}), but the next primary {} "
                         "has not answered yet"
                         .format(self.name,
                                 num_of_ready_nodes,
                                 next_primary_name,
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
            self._send_view_change_done_message(instance_id,
                                                      new_primary_name,
                                                      self.viewNo)
            logger.display("{} selected primary {} for instance {} (view {})"
                           .format(replica,
                                   new_primary_name,
                                   instance_id,
                                   self.viewNo),
                           extra={"cli": "ANNOUNCE",
                                  "tags": ["node-election"]})

    def _who_is_the_next_primary(self, instance_id):
        """
        Returns name of the next node which is supposed to be a new Primary
        in round-robin fashion
        """
        new_primary_id = (self.viewNo + instance_id) % self.node.totalNodes
        new_primary_name = Replica.generateName(
            nodeName=self.node.get_name_by_rank(new_primary_id),
            instId=instance_id)
        return new_primary_name

    def _send_view_change_done_message(self, instance_id, new_primary_name,
                                       view_no):
        """
        Sends ViewChangeDone message to other protocol participants
        """
        # QUESTION: Why is `ViewChangeDone` sent for all instances?
        ledger_info = []
        ledger_registry = self._ledger_manager.ledgerRegistry.items()
        for ledger_id, ledger_data in ledger_registry:
            ledger = ledger_data.ledger
            ledger_length = len(ledger)
            merkle_root = ledger.root_hash
            ledger_info.append((ledger_id, ledger_length, merkle_root))

        message = ViewChangeDone(new_primary_name,
                                 instance_id,
                                 view_no,
                                 ledger_info)
        self.send(message)
        replica_name = Replica.generateName(self.name, instance_id)
        self._mark_replica_as_changed_view(instance_id, replica_name,
                                           new_primary_name, ledger_info)

    def viewChanged(self, viewNo: int):
        """
        :param viewNo: the new view number.
        """
        if super().viewChanged(viewNo):
            for i in self._view_change_done:
                self._view_change_done[i] = {}
