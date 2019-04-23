from typing import Dict, List

from common.exceptions import LogicError
from common.serializers.serialization import pool_state_serializer

from plenum.common.constants import TXN_TYPE, POOL_LEDGER_ID

from plenum.common.request import Request
from plenum.common.txn_util import get_type
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_managers.request_manager import RequestManager
from stp_core.common.log import getlogger

logger = getlogger()


class WriteRequestManager(RequestManager):
    def __init__(self, database_manager: DatabaseManager):
        self.database_manager = database_manager
        self.request_handlers = {}  # type: Dict[int,List[WriteRequestHandler]]
        self.batch_handlers = {}  # type: Dict[int,List[BatchRequestHandler]]
        self.state_serializer = pool_state_serializer

    def register_req_handler(self, handler: WriteRequestHandler):
        if not isinstance(handler, WriteRequestHandler):
            raise LogicError
        type = handler.txn_type
        handler_list = self.request_handlers.setdefault(type, [])
        handler_list.append(handler)

    def remove_req_handlers(self, txn_type):
        del self.request_handlers[txn_type]

    def register_batch_handler(self, handler: BatchRequestHandler):
        if not isinstance(handler, BatchRequestHandler):
            raise LogicError
        type = handler.ledger_id
        handler_list = self.batch_handlers.setdefault(type, [])
        handler_list.append(handler)

    def remove_batch_handler(self, ledger_id):
        del self.batch_handlers[ledger_id]

    # WriteRequestHandler methods
    def static_validation(self, request: Request):
        handlers = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handlers is None:
            raise LogicError
        for handler in handlers:
            handler.static_validation(request)

    def dynamic_validation(self, request: Request):
        handlers = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handlers is None:
            raise LogicError
        for handler in handlers:
            handler.dynamic_validation(request)

    def update_state(self, txn, isCommitted=False):
        handlers = self.request_handlers.get(get_type(txn), None)
        if handlers is None:
            raise LogicError
        updated_state = None
        for handler in handlers:
            updated_state = handler.update_state([txn], updated_state, isCommitted)

    def apply_request(self, request, batch_ts):
        handlers = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handlers is None:
            raise LogicError
        start, txn, updated_state = handlers[0].apply_request(request, batch_ts, None)
        for handler in handlers[1:]:
            _, _, updated_state = handler.apply_request(request, batch_ts, updated_state)
        return start, txn

    def apply_forced_request(self, request):
        handlers = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handlers is None:
            raise LogicError
        for handler in handlers:
            handler.apply_forced_request(request)

    def revert_request(self):
        pass

    # BatchRequestHandler methods
    def post_apply_batch(self, three_pc_batch):
        handlers = self.batch_handlers.get(three_pc_batch.ledger_id, None)
        if handlers is None:
            raise LogicError
        prev_handler_result = handlers[0].post_batch_applied(three_pc_batch, None)
        for handler in handlers[1:]:
            prev_handler_result = handler.post_batch_applied(three_pc_batch, prev_handler_result)

    # TODO: no need to pass all these arguments explicitly here
    # we can use LedgerUncommittedTracker to get this values
    def commit_batch(self, ledger_id, txn_count, state_root, txn_root, pp_time):
        handlers = self.batch_handlers.get(ledger_id, None)
        if handlers is None:
            raise LogicError
        prev_handler_result = commited_txns = handlers[0].commit_batch(ledger_id, txn_count, state_root, txn_root, pp_time, None)
        for handler in handlers[1:]:
            handler.commit_batch(ledger_id, txn_count, state_root, txn_root, pp_time, prev_handler_result)
        return commited_txns

    def post_batch_rejected(self, ledger_id):
        handlers = self.batch_handlers.get(ledger_id, None)
        if handlers is None:
            raise LogicError
        prev_handler_result = handlers[0].post_batch_rejected(ledger_id, None)
        for handler in handlers[1:]:
            handler.post_batch_rejected(ledger_id, prev_handler_result)

    def transform_txn_for_ledger(self, txn):
        handlers = self.request_handlers.get(get_type(txn), None)
        if handlers is None:
            raise LogicError
        return handlers[0].transform_txn_for_ledger(txn)

    @property
    def pool_state(self):
        return self.database_manager.get_database(POOL_LEDGER_ID).state

    def get_node_data(self, nym, isCommitted: bool = True):
        key = nym.encode()
        data = self.pool_state.get(key, isCommitted)
        if not data:
            return {}
        return self.state_serializer.deserialize(data)

    def get_all_node_data_for_root_hash(self, root_hash):
        leaves = self.pool_state.get_all_leaves_for_root_hash(root_hash)
        raw_node_data = leaves.values()
        nodes = list(map(lambda x: self.state_serializer.deserialize(
            self.pool_state.get_decoded(x)), raw_node_data))
        return nodes
