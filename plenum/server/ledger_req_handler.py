from abc import ABCMeta, abstractmethod
from typing import List

from common.exceptions import PlenumValueError, LogicError
from common.serializers.serialization import state_roots_serializer
from stp_core.common.log import getlogger

from plenum.common.ledger import Ledger
from plenum.common.request import Request
from plenum.server.req_handler import RequestHandler
from plenum.common.txn_util import reqToTxn, append_txn_metadata

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

    def gen_txn_path(self, txn):
        return None

    def _reqToTxn(self, req: Request):
        return reqToTxn(req)

    def apply(self, req: Request, cons_time: int):
        txn = self._reqToTxn(req)

        txn = append_txn_metadata(txn, txn_id=self.gen_txn_path(txn))

        self.ledger.append_txns_metadata([txn], cons_time)
        (start, end), _ = self.ledger.appendTxns(
            [self.transform_txn_for_ledger(txn)])
        self.updateState([txn])
        return start, txn

    def commit(self, txnCount, stateRoot, txnRoot, ppTime) -> List:
        """
        :param txnCount: The number of requests to commit (The actual requests
        are picked up from the uncommitted list from the ledger)
        :param stateRoot: The state trie root after the txns are committed
        :param txnRoot: The txn merkle root after the txns are committed

        :return: list of committed transactions
        """

        return self._commit(self.ledger, self.state, txnCount, stateRoot,
                            txnRoot, ppTime, ts_store=self.ts_store)

    def applyForced(self, req: Request):
        if not req.isForced():
            raise LogicError('requestHandler.applyForce method is called '
                             'for not forced request: {}'.format(req))

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

    @staticmethod
    def _commit(ledger, state, txnCount, stateRoot, txnRoot, ppTime, ts_store=None):
        _, committedTxns = ledger.commitTxns(txnCount)
        stateRoot = state_roots_serializer.deserialize(stateRoot.encode()) if isinstance(
            stateRoot, str) else stateRoot
        # TODO test for that
        if ledger.root_hash != txnRoot:
            # Probably the following fail should trigger catchup
            # TODO add repr / str for Ledger class and dump it here as well
            raise PlenumValueError(
                'txnRoot', txnRoot,
                ("equal to current ledger root hash {}"
                 .format(ledger.root_hash))
            )
        state.commit(rootHash=stateRoot)
        if ts_store:
            ts_store.set(ppTime, stateRoot)
        return committedTxns

    @property
    def operation_types(self) -> set:
        return self.write_types.union(self.query_types)

    @property
    def valid_txn_types(self) -> set:
        return self.write_types.union(self.query_types)
