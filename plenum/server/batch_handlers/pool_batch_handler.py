from plenum.common.constants import POOL_LEDGER_ID
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.database_manager import DatabaseManager


class PoolBatchHandler(BatchRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, POOL_LEDGER_ID)

    def post_batch_applied(self, ledger_id, state_root, pp_time, prev_result=None):
        pass

    def post_batch_rejected(self, ledger_id):
        pass
