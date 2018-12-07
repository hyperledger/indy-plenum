from abc import abstractmethod

from plenum.common.request import Request
from plenum.server.req_handlers.handler_interfaces.handler import Handler


class ActionHandler(Handler):
    operation_type = int

    @abstractmethod
    def validate(self, request: Request):
        pass

    @abstractmethod
    def execute(self, identifier, req_id, operation):
        pass
