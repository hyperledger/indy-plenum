import copy
from collections import OrderedDict
from typing import Tuple, Dict, List

from common.exceptions import LogicError
from plenum.common.constants import TXN_TYPE, NODE, TARGET_NYM, ALIAS, SERVICES, VALIDATOR, DATA, POOL_LEDGER_ID, \
    AUDIT_LEDGER_ID, AUDIT_TXN_PRIMARIES, AUDIT_TXN_VIEW_NO
from plenum.common.txn_util import get_payload_data, get_seq_no
from plenum.common.util import getMaxFailures
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.pool_manager import TxnPoolManager


class FuturePrimariesBatchHandler(BatchRequestHandler):
    # This class is needed for correct primaries storing in audit ledger.

    def __init__(self, database_manager, node):
        super().__init__(database_manager, POOL_LEDGER_ID)
        # map of view_no and list of primaries
        self.primaries = {}    # type: Dict[int, List]
        self.db_manager = database_manager
        self.node = node

    def set_primaries(self, view_no, ps):
        self.primaries[view_no] = ps

    def _get_previous_primaries(self, audit, seq_no_end, view_no):
        if seq_no_end == 0:
            return None
        previous_seq_no = seq_no_end - 1
        previous_txn = audit.getBySeqNo(previous_seq_no)
        p_view_no = get_payload_data(previous_txn)[AUDIT_TXN_VIEW_NO]
        p_primaries = get_payload_data(previous_txn)[AUDIT_TXN_PRIMARIES]
        if p_view_no != view_no:
            if isinstance(p_primaries, int):
                # primaries is delta
                return self._get_previous_primaries(audit, previous_seq_no - p_primaries, view_no)
            return self._get_previous_primaries(audit, previous_seq_no - 1, view_no)
        else:
            if isinstance(p_primaries, int):
                p_txn = audit.getBySeqNo(previous_seq_no - p_primaries)
                return get_payload_data(p_txn)[AUDIT_TXN_PRIMARIES]
            return p_primaries

    def get_primaries_from_audit(self, view_no):
        audit_ledger = self.db_manager.get_ledger(AUDIT_LEDGER_ID)
        return self._get_previous_primaries(audit_ledger, audit_ledger.size, view_no)

    def post_batch_applied(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        # node_reg = list(self.node.nodeReg.keys())
        # number_of_inst = getMaxFailures(len(node_reg)) + 1
        view_no = self.node.viewNo if three_pc_batch.original_view_no is None else three_pc_batch.original_view_no
        if view_no not in self.primaries:
            ps = self.get_primaries_from_audit(view_no)
            if not ps:
                raise LogicError("Cannot restore primaries from Audit Ledger")
            self.primaries[view_no] = ps
        primaries = self.primaries.get(view_no)
        three_pc_batch.primaries = primaries

        # validators = TxnPoolManager.calc_node_names_ordered_by_rank(node_reg, copy.deepcopy(self.node.nodeIds))
        # three_pc_batch.primaries = self.node.primaries_selector.select_primaries(view_no=view_no,
        #                                                                          instance_count=number_of_inst,
        #                                                                          validators=validators)
        return three_pc_batch.primaries

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        pass

    def commit_batch(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        pass
