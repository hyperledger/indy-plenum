from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.database_manager import DatabaseManager
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID


class AuctionBatchHandler(BatchRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, AUCTION_LEDGER_ID)

    def post_batch_applied(self, three_pc_batch, prev_handler_result=None):
        pass

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        pass
