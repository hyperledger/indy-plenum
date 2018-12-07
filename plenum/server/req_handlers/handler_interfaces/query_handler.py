from abc import abstractmethod

from plenum.server.req_handlers.handler_interfaces.handler import Handler


class QueryHandler(Handler):

    @abstractmethod
    def query(self, request):
        pass

    @abstractmethod
    def static_validation(self, identifier, req_id, operation):
        pass
