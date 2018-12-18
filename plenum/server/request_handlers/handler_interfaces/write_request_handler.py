from abc import ABCMeta, abstractmethod
from typing import List

from common.exceptions import PlenumValueError, LogicError
from common.serializers.serialization import state_roots_serializer
from plenum.common.constants import TXN_TYPE
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.request_handler import RequestHandler
from stp_core.common.log import getlogger

from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, append_txn_metadata

logger = getlogger()


class WriteRequestHandler(RequestHandler, metaclass=ABCMeta):
    """
    Base class for request handlers
    Declares methods for validation, application of requests and
    state control
    """

    def __init__(self, config, database_manager: DatabaseManager, txn_type, ledger_id):
        self.config = config
        self.database_manager = database_manager
        self.txn_type = txn_type
        self.ledger_id = ledger_id

    @abstractmethod
    def static_validation(self, request: Request):
        pass

    @abstractmethod
    def dynamic_validation(self, req: Request):
        pass

    def apply_request(self, request: Request, batch_ts):
        txn = self._reqToTxn(request)
        txn = append_txn_metadata(txn, txn_id=self.gen_txn_path(txn))

        self.ledger.append_txns_metadata([txn], batch_ts)
        (start, end), _ = self.ledger.appendTxns(
            [self.transform_txn_for_ledger(txn)])
        self.updateState([txn])
        return start, txn

    @property
    def state(self):
        return self.database_manager.get_database(self.ledger_id).state

    @property
    def ledger(self):
        return self.database_manager.get_database(self.ledger_id).ledger

    def revert_request(self, request: Request, batch_ts):
        pass

    def updateState(self, txns, isCommitted=False):
        """
        Updates current state with a number of committed or
        not committed transactions
        """

    def gen_txn_path(self, txn):
        return None

    def _reqToTxn(self, req: Request):
        return reqToTxn(req)

    def apply_forced_request(self, req: Request):
        if not req.isForced():
            raise LogicError('requestHandler.applyForce method is called '
                             'for not forced request: {}'.format(req))

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
