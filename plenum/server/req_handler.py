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

    def __init__(self, ledger: Ledger, state: State):
        self.ledger = ledger
        self.state = state

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

    def updateState(self, txns, isCommitted=False):
        """
        Updates current state with a number of committed or
        not committed transactions
        """

    def commit(self, txnCount, stateRoot, txnRoot, ppTime) -> List:
        """
        :param txnCount: The number of requests to commit (The actual requests
        are picked up from the uncommitted list from the ledger)
        :param stateRoot: The state trie root after the txns are committed
        :param txnRoot: The txn merkle root after the txns are committed

        :return: list of committed transactions
        """

        return self._commit(self.ledger, self.state, txnCount, stateRoot,
                            txnRoot, ppTime)

    def onBatchCreated(self, state_root):
        pass

    def onBatchRejected(self):
        pass

    def is_query(self, txn_type):
        return txn_type in self.query_types

    def get_query_response(self, request):
        raise NotImplementedError

    @staticmethod
    def transform_txn_for_ledger(txn):
        return txn

    @staticmethod
    def _commit(ledger, state, txnCount, stateRoot, txnRoot, ppTime,
                ignore_txn_root_check=False):
        (seqNoStart, seqNoEnd), committedTxns = ledger.commitTxns(txnCount)
        stateRoot = base58.b58decode(stateRoot.encode()) if isinstance(
            stateRoot, str) else stateRoot
        if not ignore_txn_root_check:
            # Probably the following assertion fail should trigger catchup
            assert ledger.root_hash == txnRoot, '{} {}'.format(ledger.root_hash,
                                                               txnRoot)
        state.commit(rootHash=stateRoot)
        return txnsWithSeqNo(seqNoStart, seqNoEnd, committedTxns)

    @property
    def valid_txn_types(self) -> set:
        return self.write_types.union(self.query_types)
