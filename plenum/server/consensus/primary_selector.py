from abc import ABCMeta, abstractmethod
from typing import List

from plenum.server.batch_handlers.node_reg_handler import NodeRegHandler
from stp_core.common.log import getlogger

logger = getlogger()


class PrimariesSelector(metaclass=ABCMeta):

    @abstractmethod
    def select_primaries(self, view_no: int, instance_count: int) -> List[str]:
        pass


class RoundRobinConstantNodesPrimariesSelector(PrimariesSelector):

    def __init__(self, validators: List[str]) -> None:
        self.validators = validators

    def select_primaries(self, view_no: int, instance_count: int) -> List[str]:
        return self.select_primaries_round_robin(view_no, instance_count, self.validators)

    @staticmethod
    def select_primaries_round_robin(view_no: int, instance_count: int, validators: List[str]):
        primaries = []
        for i in range(instance_count):
            primaries.append(validators[(view_no + i) % len(validators)])
        return primaries


class RoundRobinNodeRegPrimariesSelector(PrimariesSelector):

    def __init__(self, node_reg_handler: NodeRegHandler) -> None:
        self.node_reg_handler = node_reg_handler

    def select_primaries(self, view_no: int, instance_count: int) -> List[str]:
        view_no_for_selection = view_no - 1 if view_no > 1 else 0
        return RoundRobinConstantNodesPrimariesSelector. \
            select_primaries_round_robin(view_no, instance_count,
                                         self.node_reg_handler.node_reg_at_beginning_of_view[view_no_for_selection])
