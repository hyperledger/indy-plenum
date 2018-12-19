from plenum.common.constants import POOL_LEDGER_ID
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.database_manager import DatabaseManager


class PoolBatchHandler(BatchRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, POOL_LEDGER_ID)

    def post_apply_batch(self, state_root):
        pass

    def commit_batch(self, txnCount, stateRoot, txnRoot, ppTime):
        return super().commit_batch(txnCount, stateRoot, txnRoot, ppTime)

    def revert_batch(self):
        pass
