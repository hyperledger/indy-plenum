from typing import Optional

from plenum.common.constants import AUDIT_LEDGER_ID, AUDIT
from plenum.common.exceptions import InvalidClientRequest
from plenum.common.request import Request
from plenum.server.batch_handlers.audit_batch_handler import AuditBatchHandler
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler


class AuditTxnHandler(WriteRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, AUDIT, AUDIT_LEDGER_ID)

    def static_validation(self, request: Request):
        raise InvalidClientRequest(request.identifier, request.reqId,
                                   "External audit requests are not allowed")

    def dynamic_validation(self, request: Request, req_pp_time: Optional[int]):
        pass

    def update_state(self, txn, prev_result, request, is_committed=False):
        pass

    def transform_txn_for_ledger(self, txn):
        return AuditBatchHandler.transform_txn_for_ledger(txn)
