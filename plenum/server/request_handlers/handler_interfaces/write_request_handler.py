from abc import ABCMeta, abstractmethod

from common.exceptions import LogicError
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.request_handler import RequestHandler
from plenum.server.request_handlers.utils import decode_state_value
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

    def __init__(self, database_manager: DatabaseManager, txn_type, ledger_id):
        super().__init__(database_manager, txn_type, ledger_id)

    @abstractmethod
    def static_validation(self, request: Request):
        pass

    @abstractmethod
    def dynamic_validation(self, request: Request):
        pass

    def apply_request(self, request: Request, batch_ts, prev_result):
        self._validate_request_type(request)
        txn = self._req_to_txn(request)
        txn = append_txn_metadata(txn, txn_id=self.gen_txn_id(txn))
        # TODO: try to not pass list of one txn if possible
        self.ledger.append_txns_metadata([txn], batch_ts)

        (start, end), _ = self.ledger.appendTxns(
            [self.transform_txn_for_ledger(txn)])
        updated_state = self.update_state(txn, prev_result, request)
        return start, txn, updated_state

    def revert_request(self, request: Request, batch_ts):
        pass

    @abstractmethod
    def update_state(self, txn, prev_result, request, is_committed=False):
        """
        Updates current state with a number of committed or
        not committed transactions
        :param request:
        """
        pass

    def gen_txn_id(self, txn):
        return None

    def get_from_state(self, path, is_committed=False):
        """
        Queries state for data on specified path

        :param path: path to data
        :param is_committed: queries the committed state root if True else the uncommitted root
        :return: decode_state_value
        """
        if path is None:
            return

        if isinstance(path, str):
            path = path.encode()

        encoded = self.state.get(path, isCommitted=is_committed)
        return self._decode_state_value(encoded)

    def _req_to_txn(self, req: Request):
        return reqToTxn(req)

    def apply_forced_request(self, req: Request):
        if not req.isForced():
            raise LogicError('requestHandler.applyForce method is called '
                             'for not forced request: {}'.format(req))

    @staticmethod
    def transform_txn_for_ledger(txn):
        """
        Some transactions need to be updated before they can be stored in the
        ledger, eg. storing certain payload in another data store and only its
        hash in the ledger
        """
        return txn

    def _decode_state_value(self, encoded):
        if encoded:
            value, last_seq_no, last_update_time = decode_state_value(encoded)
            return value, last_seq_no, last_update_time
        return None, None, None
