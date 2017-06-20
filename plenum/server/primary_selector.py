from typing import Iterable, List, Optional, Tuple

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
        self._view_change_done = {}  # replica name -> data
        self._ledger_manager = self.node.ledgerManager
        # Set when an appropriate view change quorum is found which has
        # sufficient same ViewChangeDone messages
        self.primary_verified = False

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
        if self.name in self._view_change_done:
            new_primary_replica_name, ledger_info = self._view_change_done[self.name]
            messages.append(ViewChangeDone(new_primary_replica_name,
                                           self.viewNo, ledger_info))
        return messages

    # overridden method of PrimaryDecider
    def decidePrimaries(self):
        if self.node.is_synced and self.master_replica.isPrimary is None:
            self._send_view_change_done_message()
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

        new_primary_name = msg.name
        ledger_info = msg.ledgerInfo

        if new_primary_name == self.previous_master_primary:
            self.discard(msg,
                         '{} got Primary from {} for {} who was primary of '
                         'master in previous view too'
                         .format(self, sender, new_primary_name),
                         logMethod=logger.warning)
            return False

        # if not self._track_view_change_done(sender,
        #                                     new_primary_name,
        #                                     ledger_info):
        #     self.discard(msg,
        #                  "already marked {} as done view change".
        #                  format(sender),
        #                  logger.warning)
        #     return False

        # Since a node can send ViewChangeDone more than one time
        self._track_view_change_done(sender,
                                     new_primary_name,
                                     ledger_info)

        if self.master_replica.hasPrimary:
            self.discard(msg,
                         "it already decided primary which is {}".
                         format(self.master_replica.primaryName),
                         logger.debug)
            return False

        # TODO: Result of `has_acceptable_view_change_quorum` should be cached
        if not self.has_acceptable_view_change_quorum:
            return False

        rv = self.has_sufficient_same_view_change_done_messages
        if rv is None:
            return False

        if self._verify_primary(*rv):
            self._startSelection()
        else:
            logger.debug('{} found failure in primary verification'.format(self))

    def _verify_primary(self, new_primary, ledger_info):
        """
        This method is called when sufficient number of ViewChangeDone
        received and makes steps to switch to the new primary
        """

        # TODO: implement case when we get equal number of ViewChangeDone
        # with different primaries specified

        expected_primary = self.next_primary_node_name(0)
        if new_primary != expected_primary:
            logger.error("{} expected next primary to be {}, but majority "
                           "declared {} instead for view {}"
                           .format(self.name, expected_primary, new_primary,
                                   self.viewNo))
            return False

        self.primary_verified = True
        return True
        # TODO: check if ledger status is expected

    def _track_view_change_done(self, sender_name, new_primary_name,
                                ledger_info):
        data = (new_primary_name, ledger_info)
        self._view_change_done[sender_name] = data

    @property
    def _hasViewChangeQuorum(self):
        # This method should just be present for master instance.
        """
        Checks whether 2f+1 nodes completed view change and whether one 
        of them is the next primary
        """
        num_of_ready_nodes = len(self._view_change_done)
        quorum = get_strong_quorum(f=self.f)
        diff = quorum - num_of_ready_nodes
        if diff > 0:
            logger.debug('{} needs {} ViewChangeDone messages'.format(self, diff))
            return False

        logger.info("{} got view change quorum ({} >= {})"
                     .format(self.name,
                             num_of_ready_nodes,
                             quorum))
        return True

    @property
    def has_view_change_from_primary(self) -> bool:
        next_primary_name = self.next_primary_node_name(0)

        if next_primary_name not in self._view_change_done:
            logger.debug("{} has not received ViewChangeDone from the next "
                         "primary {}".
                         format(self.name, next_primary_name))
            return False

        logger.debug('{} received ViewChangeDone from primary {}'
                     .format(self, next_primary_name))
        return True

    @property
    def has_acceptable_view_change_quorum(self):
        return self._hasViewChangeQuorum and self.has_view_change_from_primary

    @property
    def has_sufficient_same_view_change_done_messages(self) -> Optional[Tuple]:
        # Returns whether has a quorum of ViewChangeDone messages that are same
        # TODO: Does not look like optimal implementation.
        votes = self._view_change_done.values()
        votes = [(nm, tuple(tuple(i) for i in info)) for nm, info in votes]
        new_primary, ledger_info = mostCommonElement(votes)
        if votes.count((new_primary, ledger_info)) >= get_strong_quorum(self.f):
            logger.debug('{} found acceptable primary {} and ledger info {}'.
                         format(self, new_primary, ledger_info))
            return new_primary, ledger_info
        else:
            logger.debug('{} does not have acceptable primary'.format(self))
            return None

    def _startSelection(self):
        if not self.node.is_synced:
            logger.info('{} cannot start primary selection since mode is {}'
                        .format(self, self.node.mode))
            return
        if not self.primary_verified:
            logger.info('{} cannot start primary selection since primary is '
                        'not verified yet, this can happen due to lack of '
                        'appropriate ViewChangeDone messages'
                        .format(self))
            return

        logger.debug("{} starting selection".format(self))
        for instance_id, replica in enumerate(self.replicas):
            if replica.primaryName is not None:
                logger.debug('{} already has a primary'.format(replica))
                continue
            new_primary_name = self.next_primary_replica_name(instance_id)
            logger.display("{} selected primary {} for instance {} (view {})"
                           .format(replica,
                                   new_primary_name,
                                   instance_id,
                                   self.viewNo),
                           extra={"cli": "ANNOUNCE",
                                  "tags": ["node-election"]})

            if instance_id == 0:
                self.previous_master_primary = None

            replica.primaryChanged(new_primary_name)
            self.node.primary_selected(instance_id)

            logger.display("{} declares view change {} as completed for "
                           "instance {}, "
                           "new primary is {}, "
                           "ledger info is {}"
                           .format(replica,
                                   self.viewNo,
                                   instance_id,
                                   new_primary_name,
                                   self.ledger_info),
                           extra={"cli": "ANNOUNCE",
                                  "tags": ["node-election"]})

    def _get_primary_id(self, view_no, instance_id):
        return (view_no + instance_id) % self.node.totalNodes

    def next_primary_node_name(self, instance_id):
        return self.node.get_name_by_rank(self._get_primary_id(
                self.viewNo, instance_id))

    def next_primary_replica_name(self, instance_id):
        """
        Returns name of the next node which is supposed to be a new Primary
        in round-robin fashion
        """
        return Replica.generateName(
            nodeName=self.next_primary_node_name(instance_id),
            instId=instance_id)

    def _send_view_change_done_message(self):
        """
        Sends ViewChangeDone message to other protocol participants
        """
        new_primary_name = self.next_primary_node_name(0)
        ledger_info = self.ledger_info
        message = ViewChangeDone(new_primary_name,
                                 self.viewNo,
                                 ledger_info)
        self.send(message)
        self._track_view_change_done(self.name,
                                     new_primary_name, ledger_info)

    def view_change_started(self, viewNo: int):
        """
        :param viewNo: the new view number.
        """
        if super().view_change_started(viewNo):
            self._view_change_done = {}
            self.primary_verified = False

    # overridden method of PrimaryDecider
    def start_election_for_instance(self, instance_id):
        raise NotImplementedError("Election can be started for "
                                  "all instances only")

    @property
    def ledger_info(self):
        return [li.ledger_summary for li in
                self._ledger_manager.ledgerRegistry.values()]
