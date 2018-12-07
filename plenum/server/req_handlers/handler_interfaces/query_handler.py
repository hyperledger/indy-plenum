from abc import abstractmethod, ABC


class QueryHandler(ABC):

    @abstractmethod
    def query(self, request):
        pass

    @abstractmethod
    def static_validation(self, identifier, req_id, operation):
        pass
