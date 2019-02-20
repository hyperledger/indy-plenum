from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.database_manager import DatabaseManager


class DomainBatchHandler(BatchRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, DOMAIN_LEDGER_ID)

    def commit_batch(self, ledger_id, txn_count, state_root, txn_root, pp_time, prev_result=None):
        commited_txns = super().commit_batch(ledger_id, txn_count, state_root, txn_root, pp_time, prev_result)
        self.database_manager.ts_store.set(pp_time, state_root)
        return commited_txns

    def post_batch_applied(self, three_pc_batch, prev_handler_result=None):
        pass

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        pass
