from common.exceptions import LogicError
from plenum.common.constants import TXN_TYPE, NODE, TARGET_NYM, ALIAS, SERVICES, VALIDATOR, DATA
from plenum.common.request import Request
from plenum.common.util import getMaxFailures


class FuturePrimaries:
    # This class is needed for correct primaries storing in audit ledger.
    # It is something like uncommitted state of primaries.
    # It emulate primaries changes when pool txn is just applied.
    # When current batches reverts, it drops it's properties to node's.
    # TODO: when pluggable request handler implemented, make this class in the same style

    def __init__(self, node, node_reg, ordered_node_ids, current_required):
        self.node = node

        self.future_node_reg = list(node_reg.keys())

        # Nodes are never delete from here, so they are in a sequential order
        self.future_ordered_node_ids = ordered_node_ids

        self.future_required_number_of_instances = self.count_required_number_of_instances()
        if self.future_required_number_of_instances != current_required:
            raise LogicError('Required number of instances is unequal')

        # List of future primaries
        self.primaries = []

    def handle_pool_request(self, request: Request):
        if request.operation.get(TXN_TYPE) == NODE \
                and request.operation.get(DATA).get(SERVICES) is not None:

            node_nym = request.operation.get(TARGET_NYM)
            node_name = request.operation.get(DATA).get(ALIAS)
            curName = self.future_ordered_node_ids.get(node_nym)
            if curName is None:
                self.future_ordered_node_ids[node_nym] = node_name
            elif curName != node_name:
                raise LogicError("Alias inconsistency")

            serv = request.operation.get(DATA).get(SERVICES)
            if VALIDATOR in serv and node_name not in self.future_node_reg:
                self.future_node_reg.append(node_name)
            elif serv == [] and node_name in self.future_node_reg:
                self.future_node_reg.remove(node_name)

            count = self.count_required_number_of_instances()
            if self.future_required_number_of_instances != count:
                self.future_required_number_of_instances = count
                self.primaries = list(self.node.elector.process_selection(
                    self.future_required_number_of_instances,
                    self.future_node_reg, self.future_ordered_node_ids))

    def revert_batches(self):
        # When batches reverted and every uncommitted txns are drops,
        # we need to set future_primaries's fields like in node
        pass

    def count_required_number_of_instances(self):
        return getMaxFailures(len(self.future_node_reg)) + 1
