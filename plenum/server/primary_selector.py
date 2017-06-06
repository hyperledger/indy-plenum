from typing import Iterable, List

from plenum.common.types import ViewChangeDone
from plenum.server.router import Route
from stp_core.common.log import getlogger
from plenum.server.primary_decider import PrimaryDecider
from plenum.server.replica import Replica
from plenum.common.util import mostCommonElement, get_strong_quorum

logger = getlogger()


# TODO: Assumes that all nodes are up. Should select only
# those nodes which are up
class PrimarySelector(PrimaryDecider):
    """
    Simple implementation of primary decider. 
    Decides on a primary in round-robin fashion.
    """

    def __init__(self, node):
        super().__init__(node)
        # Stores the last `ViewChangeDone` message sent for specific instance.
        # If no view change has happened, a node simply send a ViewChangeDone
        # with view no 0 to a newly joined node
        self.view_change_done_messages = {}
        self.previous_master_primary = None
        self._view_change_done = {}

    @property
    def routes(self) -> Iterable[Route]:
        return [(ViewChangeDone, self.processViewChangeDone)]

    def processViewChangeDone(self,
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

        instance_id = msg.instId
        sender_replica_name = Replica.generateName(sender, instance_id)
        new_primary_replica_name = msg.name
        new_primary_node_name = Replica.getNodeName(new_primary_replica_name)
        last_ordered_seq_no = msg.ordSeqNo

        if instance_id == 0 and \
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
                                                  last_ordered_seq_no):
            self.discard(msg,
                         "already marked {} as done view change".
                         format(sender_replica_name),
                         logger.warning)
            return

        # TODO: 2f+ 1
        # One of them should be next primary
        # Another one - current node


        replica = self.replicas[instance_id]  # type: Replica


        # if replica.isPrimary is not None:
        #     logger.debug("{} Primary already selected; ignoring PRIMARY msg"
        #         .format(replica))
        #     return

        if not self._hasViewChangeQuorum(instance_id):
            logger.debug("{} received ViewChangeDone from {}, "
                         "but have got no quorum yet"
                         .format(self.name, sender))
            return

        # TODO: set primaryName to None when starting view change
        if replica.hasPrimary:
            self.discard(msg,
                         "it already decided primary which is {}".
                         format(replica.primaryName),
                         logger.debug)
            return

        # TODO: implement case when we get equal number of ViewChangeDone
        # with different primaries specified. Tip: use ppSeqNo for this
        # in cases when it is possible


        primary, seqNo = mostCommonElement(self.primaryDeclarations[instId].values())


        logger.display("{} selected primary {} for instance {} "
                       "(view {})".format(replica, primary,
                                          instId, self.viewNo),
                       extra={"cli": "ANNOUNCE",
                              "tags": ["node-election"]})
        logger.debug("{} selected primary on the basis of {}".
                     format(replica,
                            self.primaryDeclarations[instId]),
                     extra={"cli": False})

        # If the maximum primary declarations are for this node
        # then make it primary
        replica.primaryChanged(primary, seqNo)

        if instId == 0:
            self.previous_master_primary = None

        # If this replica has nominated itself and since the
        # election is over, reset the flag
        if self.replicaNominatedForItself == instId:
            self.replicaNominatedForItself = None

        self.node.primary_found()

        self.scheduleElection()



    def _mark_replica_as_changed_view(self,
                                      instance_id,
                                      replica_name,
                                      new_primary_replica_name,
                                      last_ordered_seq_no):
        if instance_id not in self._view_change_done:
            self._view_change_done[instance_id] = {}
        if replica_name in self._view_change_done:
            return False
        data = (new_primary_replica_name, last_ordered_seq_no)
        self._view_change_done[instance_id][replica_name] =  data
        return True

    def _hasViewChangeQuorum(self, instance_id):
        """
        Checks whether 2f+1 nodes completed view change and whether one of them is the next primary
        
        :return: 
        """
        num_of_ready_nodes = len(self._view_change_done[instance_id])
        quorum = get_strong_quorum(f=self.f)
        quorum_achieved = num_of_ready_nodes < quorum
        if quorum_achieved:
            logger.trace("{} got view change quorum ({} >= {}) for instance {}"
                         .format(self.name,
                                 num_of_ready_nodes,
                                 quorum,
                                 instance_id))
        return quorum_achieved

    def decidePrimaries(self):  # overridden method of PrimaryDecider
        self._startSelection()

    # def _scheduleSelection(self):
    #     """
    #     Schedule election at some time in the future. Currently the election
    #     starts immediately.
    #     """
    #     self._schedule(self._startSelection)

    def _startSelection(self):
        logger.debug("{} starting selection".format(self))
        for instance_id, replica in enumerate(self.replicas):
            if replica.primaryName is not None:
                logger.debug('{} already has a primary'.format(replica))
                continue
            new_primary_id = (self.viewNo + instance_id) % self.node.totalNodes
            new_primary_name = replica.generateName(
                nodeName=self.node.get_name_by_rank(new_primary_id),
                instId=instance_id)
            logger.display("{} selected primary {} for instance {} (view {})"
                           .format(replica,
                                   new_primary_name,
                                   instance_id,
                                   self.viewNo),
                           extra={"cli": "ANNOUNCE",
                                  "tags": ["node-election"]})
            replica.primaryChanged(new_primary_name)

    def viewChanged(self, viewNo: int):
        if super().viewChanged(viewNo):
            # TODO: primary selection will be done once ledgers are caught up,
            # remove next line later
            # self._startSelection()
            pass

        # if viewNo > self.viewNo:
        #     self.viewNo = viewNo
        #     self._startSelection()
        # else:
        #     logger.warning("Provided view no {} is not greater than the "
        #                    "current view no {}".format(viewNo, self.viewNo))

    # TODO: there is no such method in super class, it should be declared
    def get_msgs_for_lagged_nodes(self) -> List[ViewChangeDone]:
        msgs = []
        for instance_id, replica in enumerate(self.replicas):
            msg = self.view_change_done_messages.get(instance_id,
                                                     ViewChangeDone(
                                                         replica.primaryName,
                                                         instance_id,
                                                         self.viewNo,
                                                         None))
            msgs.append(msg)
        return msgs

