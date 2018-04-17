from abc import ABCMeta, abstractmethod
from typing import List

import base58

from plenum.common.ledger import Ledger
from plenum.common.request import Request
from plenum.persistence.util import txnsWithSeqNo
from stp_core.common.log import getlogger

from state.state import State

logger = getlogger()


class RequestHandler(metaclass=ABCMeta):
    """
    Base class for request handlers
    Declares methods for validation, application of requests and
    state control
    """
    operation_types = set()

    @abstractmethod
    def doStaticValidation(self, request: Request):
        """
        Does static validation like presence of required fields,
        properly formed request, etc
        """

    @abstractmethod
    def validate(self, req: Request):
        """
        Does dynamic validation (state based validation) on request.
        Raises exception if request is invalid.
        """

    @abstractmethod
    def apply(self, req: Request, cons_time: int):
        """
        Applies request
        """
