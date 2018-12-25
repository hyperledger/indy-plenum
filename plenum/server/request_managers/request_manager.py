from abc import abstractmethod, ABCMeta

from plenum.common.request import Request


class RequestManager(metaclass=ABCMeta):

    @abstractmethod
    def static_validation(self, request: Request):
        pass

    @abstractmethod
    def dynamic_validation(self, request: Request):
        pass
