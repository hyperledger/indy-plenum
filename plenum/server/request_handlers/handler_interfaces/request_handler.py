from abc import ABCMeta, abstractmethod
from plenum.common.request import Request
from stp_core.common.log import getlogger

logger = getlogger()


class RequestHandler(metaclass=ABCMeta):

    @abstractmethod
    def static_validation(self, request: Request):
        """
        Does static validation like presence of required fields,
        properly formed request, etc
        """

    @abstractmethod
    def dynamic_validation(self, request: Request):
        """
        Does dynamic validation (state based validation) on request.
        Raises exception if request is invalid.
        """
