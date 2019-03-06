import copy
from collections import OrderedDict
from typing import Tuple, Dict

from common.exceptions import LogicError
from plenum.common.constants import TXN_TYPE, NODE, TARGET_NYM, ALIAS, SERVICES, VALIDATOR, DATA, POOL_LEDGER_ID
from plenum.common.util import getMaxFailures
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch


class NodeState:
    def __init__(self, node_reg, node_ids, primaries):
        self.node_reg = node_reg

        # Nodes are never delete from here, so they are in a sequential order
        self.node_ids = node_ids

        # List of future primaries
        self.primaries = primaries

        self.number_of_inst = FuturePrimariesBatchHandler. \
            get_required_number_of_instances(len(node_reg))


class FuturePrimariesBatchHandler(BatchRequestHandler):
    # This class is needed for correct primaries storing in audit ledger.
    # It is something like uncommitted state of primaries.
    # It emulate primaries changes when pool txn is just applied.
    # When current batches reverts, it drops it's properties to node's.

    def __init__(self, database_manager, node):
        super().__init__(database_manager, POOL_LEDGER_ID)
        self.node = node
        self.node_states = OrderedDict()  # type: Dict[Tuple[int, int], NodeState]

    def post_batch_applied(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        node_txn_count = 0
        last_state = None
        if len(self.node_states) == 0 or self.node.new_future_primaries_needed:
            last_state = self.get_current_node_state()
            self.node.new_future_primaries_needed = False
        else:
            last_state = copy.deepcopy(self.node_states[next(reversed(self.node_states))])

        new_node_state = copy.deepcopy(last_state) \
            if len(self.node_states) != 0 else self.get_current_node_state()

        for digest in three_pc_batch.valid_digests:
            if digest not in self.node.requests:
                raise LogicError('Request is absent when it is applying')
            request = self.node.requests[digest].request
            if request.operation.get(TXN_TYPE) == NODE \
                    and request.operation.get(DATA).get(SERVICES) is not None:
                node_txn_count += 1
                node_nym = request.operation.get(TARGET_NYM)
                node_name = request.operation.get(DATA).get(ALIAS)
                curName = new_node_state.node_ids.get(node_nym)
                if curName is None:
                    new_node_state.node_ids[node_nym] = node_name
                elif curName != node_name:
                    raise LogicError("Alias inconsistency")

                serv = request.operation.get(DATA).get(SERVICES)
                if VALIDATOR in serv and node_name not in new_node_state.node_reg:
                    new_node_state.node_reg.append(node_name)
                elif serv == [] and node_name in new_node_state.node_reg:
                    new_node_state.node_reg.remove(node_name)

                count = self.get_required_number_of_instances(len(new_node_state.node_reg))
                if new_node_state.number_of_inst != count:
                    new_node_state.number_of_inst = count
                    new_node_state.primaries = self.node.elector.process_selection(
                        new_node_state.number_of_inst,
                        new_node_state.node_reg, new_node_state.node_ids)

        # We will save node state at every pool batch, so we could revert it correctly
        self.node_states[(three_pc_batch.view_no, three_pc_batch.pp_seq_no)] = new_node_state
        return new_node_state.primaries

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        if len(self.node_states) == 0:
            raise LogicError('We cannot revert pool txn if we did not applied it')
        del self.node_states[next(reversed(self.node_states))]

    def commit_batch(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        # When batch committed, we need to remove node_states of this batch
        del self.node_states[(three_pc_batch.view_no, three_pc_batch.pp_seq_no)]

    def get_current_node_state(self):
        return NodeState(list(self.node.nodeReg.keys()),
                         copy.deepcopy(self.node.nodeIds),
                         copy.deepcopy(self.node.primaries))

    @staticmethod
    def get_required_number_of_instances(nodes_count):
        return getMaxFailures(nodes_count) + 1
