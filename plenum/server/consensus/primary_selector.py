from abc import ABCMeta, abstractmethod
from typing import List

from common.exceptions import LogicError
from plenum.server.batch_handlers.node_reg_handler import NodeRegHandler
from stp_core.common.log import getlogger

logger = getlogger()


class PrimariesSelector(metaclass=ABCMeta):

    @abstractmethod
    def select_primaries(self, view_no: int) -> List[str]:
        pass


class RoundRobinConstantNodesPrimariesSelector(PrimariesSelector):

    def __init__(self, validators: List[str]) -> None:
        self.validators = validators

    def select_primaries(self, view_no: int) -> List[str]:
        return self.select_primaries_round_robin(view_no, self.validators)

    @staticmethod
    def select_primaries_round_robin(view_no: int, validators: List[str]):
        primaries = []
        N = len(validators)
        F = (N - 1) // 3
        instance_count = F + 1
        for i in range(instance_count):
            primaries.append(validators[(view_no + i) % len(validators)])
        return primaries


class RoundRobinNodeRegPrimariesSelector(PrimariesSelector):

    def __init__(self, node_reg_handler: NodeRegHandler) -> None:
        self.node_reg_handler = node_reg_handler

    def select_primaries(self, view_no: int) -> List[str]:
        view_no_for_selection = view_no - 1 if view_no > 1 else 0

        # it's possible that there is no nodeReg for some views if no txns have been ordered there
        while view_no_for_selection > 0 and view_no_for_selection not in self.node_reg_handler.node_reg_at_beginning_of_view:
            view_no_for_selection -= 1

        if view_no_for_selection not in self.node_reg_handler.node_reg_at_beginning_of_view:
            raise LogicError("Can not find view_no {} in node_reg_at_beginning_of_view {}".format(view_no,
                                                                                                  self.node_reg_handler.node_reg_at_beginning_of_view))

        return RoundRobinConstantNodesPrimariesSelector. \
            select_primaries_round_robin(view_no,
                                         self.node_reg_handler.node_reg_at_beginning_of_view[view_no_for_selection])
