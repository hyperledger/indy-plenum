from abc import ABCMeta, abstractmethod
from typing import List

import base58

from plenum.common.ledger import Ledger
from plenum.common.request import Request
from plenum.persistence.util import txnsWithSeqNo
from plenum.server.req_handler import RequestHandler
from stp_core.common.log import getlogger
from storage.state_ts_store import StateTsDbStorage

from state.state import State

logger = getlogger()


class LedgerRequestHandler(RequestHandler, metaclass=ABCMeta):
    """
    Base class for request handlers
    Declares methods for validation, application of requests and
    state control
    """

    query_types = set()
    write_types = set()

    def __init__(self, ledger: Ledger, state: State, ts_store=None):
        self.state = state
        self.ledger = ledger
        self.ts_store = ts_store

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

        (seqNoStart, seqNoEnd), committedTxns = \
            self.ledger.commitTxns(txnCount)
        stateRoot = base58.b58decode(stateRoot.encode())
        # Probably the following assertion fail should trigger catchup
        assert self.ledger.root_hash == txnRoot, '{} {}'.format(
            self.ledger.root_hash, txnRoot)
        self.state.commit(rootHash=stateRoot)
        if self.ts_store:
            self.ts_store.set(ppTime, stateRoot)
        return txnsWithSeqNo(seqNoStart, seqNoEnd, committedTxns)

    def onBatchCreated(self, state_root):
        pass

    def onBatchRejected(self):
        pass

    @abstractmethod
    def doStaticValidation(self, request: Request):
        pass

    def is_query(self, txn_type):
        return txn_type in self.query_types

    def get_query_response(self, request):
        raise NotImplementedError

    @staticmethod
    def transform_txn_for_ledger(txn):
        return txn

    @property
    def operation_types(self) -> set:
        return self.write_types.union(self.query_types)
