from typing import Iterable, List, Optional, Tuple

from orderedset import OrderedSet

from common.exceptions import LogicError
from plenum.common.constants import AUDIT_LEDGER_ID, AUDIT_TXN_PRIMARIES, AUDIT_TXN_VIEW_NO, PRIMARY_SELECTION_PREFIX
from plenum.common.messages.node_messages import ViewChangeDone
from plenum.common.txn_util import get_payload_data, get_seq_no
from plenum.server.router import Route
from stp_core.common.log import getlogger
from plenum.server.primary_decider import PrimaryDecider
from plenum.server.replica import Replica

logger = getlogger()


class PrimarySelector(PrimaryDecider):
    """
    Simple implementation of primary decider.
    Decides on a primary in round-robin fashion.
    Assumes that all nodes are up
    """

    def __init__(self, node):
        super().__init__(node)

    @property
    def routes(self) -> Iterable[Route]:
        return []

    # overridden method of PrimaryDecider
    def get_msgs_for_lagged_nodes(self) -> List[ViewChangeDone]:
        return self.node.view_changer.get_msgs_for_lagged_nodes()

    # overridden method of PrimaryDecider
    def decidePrimaries(self):
        return self.node.view_changer.decidePrimaries()

    # Question: Master is always 0, until we change that rule why incur cost
    # of a method call, also name is confusing
    def _is_master_instance(self, instance_id):
        # TODO: get master instance from outside
        # Instance 0 is always master
        return instance_id == 0

    def _get_master_primary_id(self, view_no, total_nodes):
        return view_no % total_nodes

    def _next_primary_node_name_for_master(self, node_reg, node_ids):
        rank = self._get_master_primary_id(self.viewNo, len(node_reg))
        name = self.node.get_name_by_rank(rank, node_reg, node_ids)

        # TODO add more tests or refactor
        # to return name and rank at once and remove assert
        assert name, "{} failed to get next primary node name for master instance".format(self)
        logger.trace(
            "{} selected {} as next primary node for master instance, "
            "viewNo {} with rank {}, nodeReg {}".format(self, name, self.viewNo, rank, node_reg))
        return name

    def next_primary_replica_name_for_master(self, node_reg, node_ids):
        """
        Returns name and corresponding instance name of the next node which
        is supposed to be a new Primary. In fact it is not round-robin on
        this abstraction layer as currently the primary of master instance is
        pointed directly depending on view number, instance id and total
        number of nodes.
        But since the view number is incremented by 1 before primary selection
        then current approach may be treated as round robin.
        """
        name = self._next_primary_node_name_for_master(node_reg, node_ids)
        return name, Replica.generateName(nodeName=name, instId=0)

    def next_primary_replica_name_for_backup(self, instance_id, master_primary_rank,
                                             primaries, node_reg, node_ids):
        """
        Returns name and corresponding instance name of the next node which
        is supposed to be a new Primary for backup instance in round-robin
        fashion starting from primary of master instance.
        """
        if node_reg is None:
            node_reg = self.node.nodeReg
        total_nodes = len(node_reg)
        rank = (master_primary_rank + 1) % total_nodes
        name = self.node.get_name_by_rank(rank, node_reg, node_ids)
        while name in primaries:
            rank = (rank + 1) % total_nodes
            name = self.node.get_name_by_rank(rank, node_reg, node_ids)
        return name, Replica.generateName(nodeName=name, instId=instance_id)

    # overridden method of PrimaryDecider
    def start_election_for_instance(self, instance_id):
        raise NotImplementedError("Election can be started for "
                                  "all instances only")

    def on_catchup_complete(self):
        # Select primaries after usual catchup (not view change)
        ledger = self.node.getLedger(AUDIT_LEDGER_ID)
        self.node.backup_instance_faulty_processor.restore_replicas()
        self.node.drop_primaries()
        if len(ledger) == 0:
            self.node.select_primaries()
        else:
            # Emulate view change start
            self.node.view_changer.previous_view_no = self.node.viewNo
            self.node.viewNo = get_payload_data(ledger.get_last_committed_txn())[AUDIT_TXN_VIEW_NO]
            self.node.view_changer.previous_master_primary = self.node.master_primary_name
            self.node.view_changer.set_defaults()

            self.node.primaries = self._get_last_audited_primaries()
            if len(self.replicas) != len(self.node.primaries):
                raise LogicError('Audit ledger has inconsistent number of nodes')
            if any(p not in self.node.nodeReg for p in self.node.primaries):
                raise LogicError('Audit ledger has inconsistent names of nodes')
            # Similar functionality to select_primaries
            for instance_id, replica in self.replicas.items():
                if instance_id == 0:
                    self.node.start_participating()
                replica.primaryChanged(
                    Replica.generateName(self.node.primaries[instance_id], instance_id))
                self.node.primary_selected(instance_id)

        # Primary propagation
        last_sent_pp_seq_no_restored = False
        for replica in self.replicas.values():
            replica.on_propagate_primary_done()
        if self.node.view_changer.previous_view_no == 0:
            last_sent_pp_seq_no_restored = \
                self.node.last_sent_pp_store_helper.try_restore_last_sent_pp_seq_no()
        if not last_sent_pp_seq_no_restored:
            self.node.last_sent_pp_store_helper.erase_last_sent_pp_seq_no()

        # Emulate view_change ending
        self.node.on_view_propagated()

    def _get_last_audited_primaries(self):
        audit = self.node.getLedger(AUDIT_LEDGER_ID)
        last_txn = audit.get_last_committed_txn()
        last_txn_prim_value = get_payload_data(last_txn)[AUDIT_TXN_PRIMARIES]

        if isinstance(last_txn_prim_value, int):
            seq_no = get_seq_no(last_txn) - last_txn_prim_value
            last_txn_prim_value = get_payload_data(audit.getBySeqNo(seq_no))[AUDIT_TXN_PRIMARIES]

        return last_txn_prim_value

    def process_selection(self, instance_count, node_reg, node_ids):
        # Select primaries for current view_no
        if instance_count == 0:
            return []

        '''
        Build a set of names of primaries, it is needed to avoid
        duplicates of primary nodes for different replicas.
        '''

        primaries = []
        primary_rank = None

        for i in range(instance_count):
            if i == 0:
                primary_name = self._next_primary_node_name_for_master(node_reg, node_ids)
                primary_rank = self.node.get_rank_by_name(primary_name, node_reg, node_ids)
                if primary_rank is None:
                    raise LogicError('primary_rank must not be None')
            else:
                primary_name, _ = self.next_primary_replica_name_for_backup(
                    i, primary_rank, primaries, node_reg, node_ids)

            primaries.append(primary_name)
            logger.display("{} selected primary {} for instance {} (view {})"
                           .format(PRIMARY_SELECTION_PREFIX,
                                   primary_name, i, self.viewNo),
                           extra={"cli": "ANNOUNCE",
                                  "tags": ["node-election"]})
        if len(primaries) != instance_count:
            raise LogicError('instances inconsistency')

        if len(primaries) != len(set(primaries)):
            raise LogicError('repeating instances')

        return primaries
