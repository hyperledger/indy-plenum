from typing import Dict, List

from common.exceptions import LogicError
from plenum.common.constants import TXN_TYPE, NODE, TARGET_NYM, ALIAS, SERVICES, VALIDATOR, DATA
from plenum.common.util import getMaxFailures
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch


class FuturePrimaries:
    # This class is needed for correct primaries storing in audit ledger.
    # It is something like uncommitted state of primaries.
    # It emulate primaries changes when pool txn is just applied.
    # When current batches reverts, it drops it's properties to node's.
    # TODO: when pluggable request handler implemented, make this class in the same style

    def __init__(self, node, node_names, ordered_node_ids, current_required):
        self.node = node

        self.future_node_names = node_names

        # Nodes are never delete from here, so they are in a sequential order
        self.future_ordered_node_ids = ordered_node_ids

        self.future_required_number_of_instances = self.count_required_number_of_instances()
        if self.future_required_number_of_instances != current_required:
            raise LogicError('Required number of instances is unequal')

        # List of future primaries
        self.future_primaries = []

    def handle_3pc_batch(self, three_pc_batch: ThreePcBatch):
        for i, req in enumerate(three_pc_batch.requests):
            if req.operation.get(TXN_TYPE) == NODE \
                    and req.operation.get(DATA).get(SERVICES) is not None \
                    and i not in three_pc_batch.invalid_indices:

                node_nym = req.operation.get(TARGET_NYM)
                node_name = req.operation.get(DATA).get(ALIAS)
                curName = self.future_ordered_node_ids.get(node_nym)
                if curName is None:
                    self.future_ordered_node_ids[node_nym] = node_name
                elif curName != node_name:
                    raise LogicError("Alias inconsistency")

                serv = req.operation.get(DATA).get(SERVICES)
                if VALIDATOR in serv and node_name not in self.future_node_names:
                    self.future_node_names.append(node_name)
                elif serv == [] and node_name in self.future_node_names:
                    self.future_node_names.remove(node_name)

                count = self.count_required_number_of_instances()
                if self.future_required_number_of_instances != \
                        count:
                    self.future_required_number_of_instances = count
                    self.reselect()
        return self.future_primaries

    def reselect(self):
        self.future_primaries = []
        # Logic similar to select_primaries in node.py
        master_primary_name, _ = self.node.elector.next_primary_replica_name_for_master(
            self.future_node_names, self.future_ordered_node_ids)
        self.future_primaries.append(master_primary_name)
        primary_rank = self.node.get_rank_by_name(master_primary_name)

        for i in range(1, self.future_required_number_of_instances):
            new_primary_name, _ = \
                self.node.elector.next_primary_replica_name_for_backup(
                    i, primary_rank, self.future_primaries)

    def revert_batches(self):
        # When batches reverted and every uncommitted txns are drops,
        # we need to set future_primaries's fields like in node
        pass

    def count_required_number_of_instances(self):
        return getMaxFailures(len(self.future_node_names)) + 1
