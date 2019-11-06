from typing import Dict, List

from common.exceptions import LogicError
from plenum.common.constants import POOL_LEDGER_ID, \
    AUDIT_LEDGER_ID, AUDIT_TXN_PRIMARIES, AUDIT_TXN_VIEW_NO
from plenum.common.ledger import Ledger
from plenum.common.txn_util import get_payload_data
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch


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

    def _inspect_audit_txn(self, txn, view_no) -> int:
        # Return delta of next primary's record
        p_view_no = get_payload_data(txn)[AUDIT_TXN_VIEW_NO]
        p_primaries = get_payload_data(txn)[AUDIT_TXN_PRIMARIES]
        if p_view_no != view_no:
            if isinstance(p_primaries, int):
                return p_primaries
            return 1
        return p_primaries

    def _get_previous_primaries(self, audit, view_no, seq_no_end):
        if seq_no_end == 0:
            return None
        previous_txn = audit.get_by_seq_no_uncommitted(seq_no_end)
        primaries = self._inspect_audit_txn(previous_txn, view_no)
        if isinstance(primaries, list):
            return primaries
        return self._get_previous_primaries(audit, view_no, seq_no_end - primaries)

    def get_primaries_from_audit(self, view_no):
        audit_ledger = self.db_manager.get_ledger(AUDIT_LEDGER_ID)
        return self._get_previous_primaries(audit_ledger, view_no, audit_ledger.uncommitted_size)

    def get_primaries(self, view_no):
        if view_no not in self.primaries:
            ps = self.get_primaries_from_audit(view_no)
            if not ps:
                return None
            self.set_primaries(view_no, ps)
        return self.primaries.get(view_no)

    def check_primaries(self, view_no, primaries_to_check):
        ps = self.get_primaries(view_no)
        if ps:
            return ps == primaries_to_check
        return False

    def post_batch_applied(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        view_no = self.node.viewNo if three_pc_batch.original_view_no is None else three_pc_batch.original_view_no
        primaries = self.get_primaries(view_no)
        if primaries is None:
            # In case of reordering after view_change
            # we can trust for list of primaries from PrePrepare
            # because this PrePrepare was validated on all the nodes
            primaries = three_pc_batch.primaries
            self.set_primaries(view_no, primaries)

        three_pc_batch.primaries = primaries
        return three_pc_batch.primaries

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        pass

    def commit_batch(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        pass
