from abc import ABCMeta, abstractmethod
from typing import List

from common.exceptions import LogicError
from plenum.server.batch_handlers.node_reg_handler import NodeRegHandler
from stp_core.common.log import getlogger

logger = getlogger()


class PrimariesSelector(metaclass=ABCMeta):

    @abstractmethod
    def select_backup_primaries(self, view_no: int, master_primary: str) -> List[str]:
        pass

    @abstractmethod
    def select_master_primary(self, view_no: int) -> str:
        pass


class RoundRobinConstantNodesPrimariesSelector(PrimariesSelector):

    def __init__(self, validators: List[str]) -> None:
        self.validators = validators

    def select_master_primary(self, view_no: int) -> str:
        return self.validators[view_no % len(self.validators)]

    def select_backup_primaries(self, view_no: int) -> List[str]:
        N = len(self.validators)
        F = (N - 1) // 3
        return self.select_backup_primaries_round_robin(view_no, self.validators, F)

    @staticmethod
    def select_backup_primaries_round_robin(view_no: int, validators: List[str], backup_instance_count: int):
        primaries = []
        for i in range(backup_instance_count):
            primaries.append(validators[(view_no + i + 1) % len(validators)])
        return primaries


class RoundRobinNodeRegPrimariesSelector(PrimariesSelector):

    def __init__(self, node_reg_handler: NodeRegHandler) -> None:
        self.node_reg_handler = node_reg_handler

    def select_master_primary(self, view_no: int) -> str:
        # Get a list of nodes to be used for selection as the one at the beginning of last view
        # to guarantee that same primaries will be selected on all nodes once view change is started.
        # Remark: It's possible that there is no nodeReg for some views if no txns have been ordered there
        view_no_for_selection = view_no - 1 if view_no > 1 else 0
        while view_no_for_selection > 0 and view_no_for_selection not in self.node_reg_handler.node_reg_at_beginning_of_view:
            view_no_for_selection -= 1
        if view_no_for_selection not in self.node_reg_handler.node_reg_at_beginning_of_view:
            raise LogicError("Can not find view_no {} in node_reg_at_beginning_of_view {}".format(view_no,
                                                                                                  self.node_reg_handler.node_reg_at_beginning_of_view))
        node_reg_to_use = self.node_reg_handler.node_reg_at_beginning_of_view[view_no_for_selection]

        return node_reg_to_use[view_no % len(node_reg_to_use)]

    def select_primaries(self, view_no: int) -> List[str]:
        # Get a list of nodes to be used for selection as the one at the beginning of last view
        # to guarantee that same primaries will be selected on all nodes once view change is started.
        # Remark: It's possible that there is no nodeReg for some views if no txns have been ordered there
        view_no_for_selection = view_no - 1 if view_no > 1 else 0
        while view_no_for_selection > 0 and view_no_for_selection not in self.node_reg_handler.node_reg_at_beginning_of_view:
            view_no_for_selection -= 1
        if view_no_for_selection not in self.node_reg_handler.node_reg_at_beginning_of_view:
            raise LogicError("Can not find view_no {} in node_reg_at_beginning_of_view {}".format(view_no,
                                                                                                  self.node_reg_handler.node_reg_at_beginning_of_view))

        node_reg_to_use = self.node_reg_handler.node_reg_at_beginning_of_view[view_no_for_selection]
        N = len(node_reg_to_use)
        F = (N - 1) // 3
        instance_count = F + 1

        return RoundRobinConstantNodesPrimariesSelector.select_primaries_round_robin(view_no,
                                                                                     node_reg_to_use,
                                                                                     instance_count)
