from typing import Iterable, List, Optional, Tuple

from plenum.common.messages.node_messages import ViewChangeDone
from plenum.server.router import Route
from stp_core.common.log import getlogger
from plenum.server.primary_decider import PrimaryDecider
from plenum.server.replica import Replica

logger = getlogger()


class PrimarySelector(PrimaryDecider):
    """
    Simple implementation of primary decider.
    Decides on a primary in round-robin fashion.
    Assumes that all nodes are up
    """

    def __init__(self, node):
        super().__init__(node)

    @property
    def routes(self) -> Iterable[Route]:
        return []

    # overridden method of PrimaryDecider
    def get_msgs_for_lagged_nodes(self) -> List[ViewChangeDone]:
        return self.node.view_changer.get_msgs_for_lagged_nodes()

    # overridden method of PrimaryDecider
    def decidePrimaries(self):
        return self.node.view_changer.decidePrimaries()

    # Question: Master is always 0, until we change that rule why incur cost
    # of a method call, also name is confusing
    def _is_master_instance(self, instance_id):
        # TODO: get master instance from outside
        # Instance 0 is always master
        return instance_id == 0

    def _get_primary_id(self, view_no, instance_id, total_nodes):
        return (view_no + instance_id) % total_nodes

    def next_primary_node_name(self, instance_id, nodeReg=None):
        if nodeReg is None:
            nodeReg = self.node.nodeReg
        rank = self._get_primary_id(self.viewNo, instance_id, len(nodeReg))
        name = self.node.get_name_by_rank(rank, nodeReg=nodeReg)

        logger.trace("{} selected {} as next primary node for instId {}, "
                     "viewNo {} with rank {}, nodeReg {}".format(
                         self, name, instance_id, self.viewNo, rank, nodeReg))
        assert name, "{} failed to get next primary node name".format(self)

        return name

    def next_primary_replica_name(self, instance_id, nodeReg=None):
        """
        Returns name of the next node which is supposed to be a new Primary
        in round-robin fashion
        """
        return Replica.generateName(
            nodeName=self.next_primary_node_name(instance_id, nodeReg=nodeReg),
            instId=instance_id)

    # overridden method of PrimaryDecider
    def start_election_for_instance(self, instance_id):
        raise NotImplementedError("Election can be started for "
                                  "all instances only")
