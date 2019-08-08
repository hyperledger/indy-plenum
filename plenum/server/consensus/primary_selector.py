from abc import ABCMeta, abstractmethod
from typing import List

from common.exceptions import LogicError
from plenum.common.constants import PRIMARY_SELECTION_PREFIX
from stp_core.common.log import getlogger

logger = getlogger()


class PrimariesSelector(metaclass=ABCMeta):

    @abstractmethod
    def select_primaries(self, view_no: int, instance_count: int, validators: List[str]) -> List[str]:
        pass


class RoundRobinPrimariesSelector(PrimariesSelector):

    def select_primaries(self, view_no: int, instance_count: int, validators: List[str]) -> List[str]:
        # Select primaries for current view_no
        if instance_count == 0:
            return []

        # Build a set of names of primaries, it is needed to avoid
        # duplicates of primary nodes for different replicas.
        primaries = []
        master_primary = None

        for i in range(instance_count):
            if i == 0:
                primary_name = self._next_primary_node_name_for_master(view_no, validators)
                master_primary = primary_name
            else:
                primary_name = self._next_primary_node_name_for_backup(master_primary, validators, primaries)

            primaries.append(primary_name)
            logger.display("{} selected primary {} for instance {} (view {})"
                           .format(PRIMARY_SELECTION_PREFIX,
                                   primary_name, i, view_no),
                           extra={"cli": "ANNOUNCE",
                                  "tags": ["node-election"]})
        if len(primaries) != instance_count:
            raise LogicError('instances inconsistency')

        if len(primaries) != len(set(primaries)):
            raise LogicError('repeating instances')

        return primaries

    def _next_primary_node_name_for_master(self, view_no: int, validators: List[str]) -> str:
        """
        Returns name and rank of the next node which is supposed to be a new Primary on master instance.
        In fact it is not round-robin on this abstraction layer as currently the primary of master instance is
        pointed directly depending on view number, instance id and total
        number of nodes.
        But since the view number is incremented by 1 before primary selection
        then current approach may be treated as round robin.
        """
        return validators[view_no % len(validators)]

    def _next_primary_node_name_for_backup(self, master_primary: str, validators: List[str],
                                           primaries: List[str]) -> str:
        """
        Returns name of the next node which
        is supposed to be a new Primary for backup instance in round-robin
        fashion starting from primary of master instance.
        """
        master_primary_rank = validators.index(master_primary)
        nodes_count = len(validators)
        rank = (master_primary_rank + 1) % nodes_count
        name = validators[rank]
        while name in primaries:
            rank = (rank + 1) % nodes_count
            name = validators[rank]
        return name
