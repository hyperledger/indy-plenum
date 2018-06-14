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

    def _get_master_primary_id(self, view_no, total_nodes):
        return view_no % total_nodes

    def _next_primary_node_name_for_master(self, nodeReg=None):
        if nodeReg is None:
            nodeReg = self.node.nodeReg
        rank = self._get_master_primary_id(self.viewNo, len(nodeReg))
        name = self.node.get_name_by_rank(rank, nodeReg=nodeReg)

        # TODO add more tests or refactor
        # to return name and rank at once and remove assert
        assert name, "{} failed to get next primary node name for master instance".format(self)
        logger.trace("{} selected {} as next primary node for master instance, "
                     "viewNo {} with rank {}, nodeReg {}".format(
                         self, name, self.viewNo, rank, nodeReg))
        return name

    def next_primary_replica_name_for_master(self, nodeReg=None):
        """
        Returns name and corresponding instance name of the next node which
        is supposed to be a new Primary. In fact it is not round-robin on
        this abstraction layer as currently the primary of master instance is
        pointed directly depending on view number, instance id and total
        number of nodes.
        But since the view number is incremented by 1 before primary selection
        then current approach may be treated as round robin.
        """
        name = self._next_primary_node_name_for_master(nodeReg)
        return name, Replica.generateName(nodeName=name, instId=0)

    def next_primary_replica_name_for_backup(self, instance_id, master_primary_rank,
                                             primaries, nodeReg=None):
        """
        Returns name and corresponding instance name of the next node which
        is supposed to be a new Primary for backup instance in round-robin
        fashion starting from primary of master instance.
        """
        if nodeReg is None:
            nodeReg = self.node.nodeReg
        total_nodes = len(nodeReg)
        rank = (master_primary_rank + 1) % total_nodes
        name = self.node.get_name_by_rank(rank, nodeReg=nodeReg)
        while name in primaries:
            rank = (rank + 1) % total_nodes
            name = self.node.get_name_by_rank(rank, nodeReg=nodeReg)
        return name, Replica.generateName(nodeName=name, instId=instance_id)

    # overridden method of PrimaryDecider
    def start_election_for_instance(self, instance_id):
        raise NotImplementedError("Election can be started for "
                                  "all instances only")
