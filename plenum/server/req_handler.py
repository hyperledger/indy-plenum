from typing import List

import base58

from plenum.common.ledger import Ledger
from plenum.common.request import Request
from plenum.persistence.util import txnsWithSeqNo
from stp_core.common.log import getlogger

from state.state import State

logger = getlogger()


class RequestHandler:
    """
    Base class for request handlers
    Declares methods for validation, application of requests and
    state control
    """
    write_types = set()
    query_types = set()
    action_types = set()

    def doStaticValidation(self, request: Request):
        """
        Does static validation like presence of required fields,
        properly formed request, etc
        """

    def validate(self, req: Request):
        """
        Does dynamic validation (state based validation) on request.
        Raises exception if request is invalid.
        """

    def apply(self, req: Request, cons_time: int):
        """
        Applies request
        """
    #
    # def onBatchCreated(self, state_root):
    #     pass
    #
    # def onBatchRejected(self):
    #     pass

    def is_query(self, txn_type):
        return txn_type in self.query_types

    def is_action(self, txn_type):
        return txn_type in self.action_types
    #
    # def get_query_response(self, request):
    #     raise NotImplementedError
    #
    # @staticmethod
    # def transform_txn_for_ledger(txn):
    #     return txn
    #
    @property
    def valid_txn_types(self) -> set:
        return self.write_types.union(self.query_types)
