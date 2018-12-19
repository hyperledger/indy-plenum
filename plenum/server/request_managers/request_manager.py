from abc import abstractmethod

from plenum.common.request import Request


class RequestManager:

    @abstractmethod
    def static_validation(self, request: Request):
        pass

    @abstractmethod
    def dynamic_validation(self, request: Request):
        pass
