from typing import Iterable, List, Optional, Tuple

from plenum.common.constants import PRIMARY_SELECTION_PREFIX, VIEW_CHANGE_PREFIX, POOL_LEDGER_ID
from plenum.common.messages.node_messages import ViewChangeDone
from plenum.server.router import Route
from stp_core.common.log import getlogger
from plenum.server.primary_decider import PrimaryDecider
from plenum.server.replica import Replica
from plenum.common.util import mostCommonElement

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
        return [(ViewChangeDone, self.node.view_changer._processViewChangeDoneMessage)]

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



    def _processViewChangeDoneMessage(self, *args, **kwargs):
        return self.node.view_changer._processViewChangeDoneMessage(*args, **kwargs)


    def _verify_primary(self, *args, **kwargs):
        return self.node.view_changer._verify_primary(*args, **kwargs)


    def _on_verified_view_change_done_msg(self, *args, **kwargs):
        return self.node.view_changer._on_verified_view_change_done_msg(*args, **kwargs)


    def _hasViewChangeQuorum(self, *args, **kwargs):
        return self.node.view_changer._hasViewChangeQuorum(*args, **kwargs)


    def is_propagated_view_change_completed(self, *args, **kwargs):
        return self.node.view_changer.is_propagated_view_change_completed(*args, **kwargs)


    def has_view_change_from_primary(self, *args, **kwargs):
        return self.node.view_changer.has_view_change_from_primary(*args, **kwargs)


    def has_acceptable_view_change_quorum(self, *args, **kwargs):
        return self.node.view_changer.has_acceptable_view_change_quorum(*args, **kwargs)


    def get_sufficient_same_view_change_done_messages(self, *args, **kwargs):
        return self.node.view_changer.get_sufficient_same_view_change_done_messages(*args, **kwargs)


    def is_behind_for_view(self, *args, **kwargs):
        return self.node.view_changer.is_behind_for_view(*args, **kwargs)


    def _start_selection(self, *args, **kwargs):
        return self.node.view_changer._start_selection(*args, **kwargs)


    def _send_view_change_done_message(self, *args, **kwargs):
        return self.node.view_changer._send_view_change_done_message(*args, **kwargs)


    def view_change_started(self, *args, **kwargs):
        return self.node.view_changer.view_change_started(*args, **kwargs)

    @property
    def ledger_summary(self):
        return self.node.view_changer.ledger_summary()


def getp(pr):
    def wrapper(self):
        return getattr(self.node.view_changer, pr)
    return wrapper

def setp(pr):
    def wrapper(self, v):
        setattr(self.node.view_changer, pr, v)
    return wrapper

for pr in (
        'previous_master_primary',
        '_view_change_done',
        '_primary_verified',
        '_has_view_change_from_primary',
        '_has_acceptable_view_change_quorum',
        '_accepted_view_change_done_message',
        '_propagated_view_change_completed',
        'quorum',
        '_hasViewChangeQuorum',
        'is_propagated_view_change_completed',
        'has_view_change_from_primary',
        'has_acceptable_view_change_quorum',
        'is_behind_for_view',
        'ledger_summary',
        '_ledger_manager'):
    setattr(PrimarySelector, pr, property(getp(pr), setp(pr)))
