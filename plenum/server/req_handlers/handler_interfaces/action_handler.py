from abc import abstractmethod, ABC


class ActionHandler(ABC):

    @abstractmethod
    def validate(self, request):
        pass

    @abstractmethod
    def execute(self, identifier, req_id, operation):
        pass


