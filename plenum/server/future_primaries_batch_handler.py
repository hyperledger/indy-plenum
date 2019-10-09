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
    # It is something like uncommitted state of primaries.
    # It emulate primaries changes when pool txn is just applied.
    # When current batches reverts, it drops it's properties to node's.

    def __init__(self, database_manager, node):
        super().__init__(database_manager, POOL_LEDGER_ID)
        self.node = node

    def post_batch_applied(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        node_reg = list(self.node.nodeReg.keys())
        node_ids = copy.deepcopy(self.node.nodeIds)
        for digest in three_pc_batch.valid_digests:
            if digest not in self.node.requests:
                raise LogicError('Request is absent when it is applying')
            request = self.node.requests[digest].request
            if request.operation.get(TXN_TYPE) == NODE \
                    and request.operation.get(DATA).get(SERVICES) is not None:
                node_nym = request.operation.get(TARGET_NYM)
                node_name = request.operation.get(DATA).get(ALIAS)
                curName = node_ids.get(node_nym)
                if curName is None:
                    node_ids[node_nym] = node_name
                elif curName != node_name:
                    raise LogicError("Alias inconsistency")

                serv = request.operation.get(DATA).get(SERVICES)
                if VALIDATOR in serv and node_name not in node_reg:
                    node_reg.append(node_name)
                elif serv == [] and node_name in node_reg:
                    node_reg.remove(node_name)

        count = self.get_required_number_of_instances(len(node_reg))

        number_of_inst = count
        new_validators = TxnPoolManager.calc_node_names_ordered_by_rank(node_reg,
                                                                        node_ids)
        view_no = self.node.viewNo if three_pc_batch.original_view_no is None else three_pc_batch.original_view_no
        three_pc_batch.primaries = self.node.primaries_selector.select_primaries(view_no=view_no,
                                                                                 instance_count=number_of_inst,
                                                                                 validators=new_validators)
        return three_pc_batch.primaries

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        pass

    def commit_batch(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        pass

    @staticmethod
    def get_required_number_of_instances(nodes_count):
        return getMaxFailures(nodes_count) + 1
