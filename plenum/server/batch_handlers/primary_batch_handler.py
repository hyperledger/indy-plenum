from plenum.common.constants import POOL_LEDGER_ID
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.node_reg_handler import NodeRegHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.consensus.primary_selector import RoundRobinNodeRegPrimariesSelector
from plenum.server.database_manager import DatabaseManager


class PrimaryBatchHandler(BatchRequestHandler):

    def __init__(self, database_manager: DatabaseManager, node_reg_handler: NodeRegHandler):
        BatchRequestHandler.__init__(self, database_manager, POOL_LEDGER_ID)
        self.primaries_selector = RoundRobinNodeRegPrimariesSelector(node_reg_handler)

    def post_batch_applied(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        view_no = three_pc_batch.original_view_no if three_pc_batch.original_view_no is not None else three_pc_batch.view_no
        three_pc_batch.primaries = self.primaries_selector.select_primaries(view_no)

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        pass

    def commit_batch(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        pass
