import copy
from collections import OrderedDict
from typing import Tuple, Dict

from common.exceptions import LogicError
from plenum.common.constants import TXN_TYPE, NODE, TARGET_NYM, ALIAS, SERVICES, VALIDATOR, DATA, POOL_LEDGER_ID
from plenum.common.util import getMaxFailures
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.pool_manager import TxnPoolManager


class FuturePrimariesBatchHandler(BatchRequestHandler):
    # This class is needed for correct primaries storing in audit ledger.

    def __init__(self, database_manager, node):
        super().__init__(database_manager, POOL_LEDGER_ID)
        self.node = node

    def post_batch_applied(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        node_reg = list(self.node.nodeReg.keys())
        number_of_inst = getMaxFailures(len(node_reg)) + 1
        view_no = self.node.viewNo if three_pc_batch.original_view_no is None else three_pc_batch.original_view_no
        validators = TxnPoolManager.calc_node_names_ordered_by_rank(node_reg, copy.deepcopy(self.node.nodeIds))
        three_pc_batch.primaries = self.node.primaries_selector.select_primaries(view_no=view_no,
                                                                                 instance_count=number_of_inst,
                                                                                 validators=validators)
        return three_pc_batch.primaries

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        pass

    def commit_batch(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        pass
