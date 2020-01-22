from abc import ABCMeta, abstractmethod
from typing import List

from common.exceptions import LogicError
from plenum.server.batch_handlers.node_reg_handler import NodeRegHandler
from stp_core.common.log import getlogger

logger = getlogger()


class PrimariesSelector(metaclass=ABCMeta):

    @abstractmethod
    def select_master_primary(self, view_no: int) -> str:
        pass

    @abstractmethod
    def select_primaries(self, view_no: int) -> List[str]:
        pass


class RoundRobinConstantNodesPrimariesSelector(PrimariesSelector):

    def __init__(self, validators: List[str]) -> None:
        self.validators = validators

    def select_master_primary(self, view_no: int) -> str:
        return self.validators[view_no % len(self.validators)]

    def select_primaries(self, view_no: int) -> List[str]:
        master_primary = self.select_master_primary(view_no)
        return [master_primary] + self._select_backup_primaries(view_no, master_primary)

    def _select_backup_primaries(self, view_no: int, master_primary) -> List[str]:
        N = len(self.validators)
        F = (N - 1) // 3
        return self.select_backup_primaries_round_robin(view_no, self.validators, F, master_primary)

    @staticmethod
    def select_backup_primaries_round_robin(view_no: int, validators: List[str], backup_instance_count: int,
                                            master_primary: str):
        primaries = []
        i = 1
        while len(primaries) < backup_instance_count:
            backup_primary = validators[(view_no + i) % len(validators)]
            if backup_primary != master_primary:
                primaries.append(backup_primary)
            i += 1
        return primaries


class RoundRobinNodeRegPrimariesSelector(PrimariesSelector):

    def __init__(self, node_reg_handler: NodeRegHandler) -> None:
        self.node_reg_handler = node_reg_handler

    def select_master_primary(self, view_no: int) -> str:
        # use committed node reg at the beginning of view to make sure that N-F nodes selected the same Primary at view change start
        return self._do_select_master_primary(view_no,
                                              self.node_reg_handler.committed_node_reg_at_beginning_of_view)

    def select_primaries(self, view_no: int) -> List[str]:
        # use uncommitted_node_reg_at_beginning_of_view to have correct primaries in audit
        master_primary = self._do_select_master_primary(view_no,
                                                        self.node_reg_handler.uncommitted_node_reg_at_beginning_of_view)
        return [master_primary] + self._select_backup_primaries(view_no, master_primary)

    def _do_select_master_primary(self, view_no: int, node_reg) -> str:
        # Get a list of nodes to be used for selection as the one at the beginning of last view
        # to guarantee that same primaries will be selected on all nodes once view change is started.
        # Remark: It's possible that there is no nodeReg for some views if no txns have been ordered there
        view_no_for_selection = view_no - 1 if view_no > 1 else 0
        while view_no_for_selection > 0 and view_no_for_selection not in node_reg:
            view_no_for_selection -= 1
        if view_no_for_selection not in node_reg:
            raise LogicError("Can not find view_no {} in node_reg_at_beginning_of_view {}".format(view_no,
                                                                                                  node_reg))
        node_reg_to_use = node_reg[view_no_for_selection]

        return node_reg_to_use[view_no % len(node_reg_to_use)]

    def _select_backup_primaries(self, view_no: int, master_primary) -> List[str]:
        N = len(self.node_reg_handler.active_node_reg)
        F = (N - 1) // 3
        return RoundRobinConstantNodesPrimariesSelector.select_backup_primaries_round_robin(view_no,
                                                                                            self.node_reg_handler.active_node_reg,
                                                                                            F,
                                                                                            master_primary)
