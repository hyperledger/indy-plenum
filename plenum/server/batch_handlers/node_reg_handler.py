from collections import deque

from plenum.common.constants import POOL_LEDGER_ID, ALIAS, SERVICES, VALIDATOR, NODE, DATA, AUDIT_LEDGER_ID, \
    AUDIT_TXN_NODE_REG, TYPE, AUDIT_TXN_VIEW_NO, AUDIT_TXN_PRIMARIES, AUDIT_TXN_LEDGERS_SIZE
from plenum.common.request import Request
from plenum.common.txn_util import get_type, get_payload_data, get_seq_no
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler


class NodeRegHandler(BatchRequestHandler, WriteRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        BatchRequestHandler.__init__(self, database_manager, POOL_LEDGER_ID)
        WriteRequestHandler.__init__(self, database_manager, NODE, POOL_LEDGER_ID)

        self.uncommitted_node_reg = []
        self.committed_node_reg = []
        self.node_reg_at_beginning_of_last_view = []
        self.node_reg_at_beginning_of_this_view = []

        self._uncommitted = deque()
        self._current_view_no = 0

    def on_catchup_finished(self):
        self._load_current_node_reg()
        self._load_last_view_node_reg()

    def post_batch_applied(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        self._uncommitted.append(list(self.uncommitted_node_reg))
        if three_pc_batch.view_no > self._current_view_no:
            self.node_reg_at_beginning_of_last_view = list(self.node_reg_at_beginning_of_this_view)
            self.node_reg_at_beginning_of_this_view = list(self.uncommitted_node_reg)
            self._current_view_no = three_pc_batch.view_no
        three_pc_batch.node_reg = self.uncommitted_node_reg

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        self._uncommitted.pop()
        if len(self._uncommitted) == 0:
            self.uncommitted_node_reg = self.committed_node_reg
        else:
            self.uncommitted_node_reg = self._uncommitted[-1]
        # TODO: revert node_reg_at_beginning_of_last_view and node_reg_at_beginning_of_this_view

    def commit_batch(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        self.committed_node_reg = self._uncommitted.popleft()

    def apply_request(self, request: Request, batch_ts, prev_result):
        if request.operation.get(TYPE) != NODE:
            return None, None, None

        node_name = request.operation[DATA][ALIAS]
        services = request.operation[DATA].get(SERVICES, [])

        if node_name not in self.uncommitted_node_reg and VALIDATOR in services:
            # new node added or old one promoted
            self.uncommitted_node_reg.append(node_name)
        elif node_name in self.uncommitted_node_reg and VALIDATOR not in services:
            # existing node demoted
            self.uncommitted_node_reg.remove(node_name)

        return None, None, None

    def update_state(self, txn, prev_result, request, is_committed=False):
        pass

    def static_validation(self, request):
        pass

    def dynamic_validation(self, request):
        pass

    def gen_state_key(self, txn):
        pass

    def _load_current_node_reg(self):
        node_reg = self.__load_current_node_reg_from_audit_ledger()
        if node_reg is None:
            node_reg = self.__load_node_reg_from_pool_ledger()
        self.uncommitted_node_reg = list(node_reg)
        self.committed_node_reg = list(node_reg)

    def _load_last_view_node_reg(self):
        # 1. check if we have audit ledger at all
        audit_ledger = self.database_manager.get_ledger(AUDIT_LEDGER_ID)
        if not audit_ledger:
            # don't have audit ledger yet, so get aleady loaded values from the pool ledger
            self.node_reg_at_beginning_of_last_view = list(self.uncommitted_node_reg)
            self.node_reg_at_beginning_of_this_view = list(self.uncommitted_node_reg)
            self._current_view_no = 0
            return

        # 2. get the first txn in the current view
        first_txn_in_this_view = self.__get_first_txn_in_view_from_audit(audit_ledger,
                                                                         audit_ledger.get_last_committed_txn())
        self._current_view_no = get_payload_data(first_txn_in_this_view)[AUDIT_TXN_VIEW_NO]
        self.node_reg_at_beginning_of_this_view = list(self.__load_node_reg_for_view(first_txn_in_this_view))

        # 3. If audit ledger has information about the current view only,
        # then let last and current view node regs be equal (and get from the pool ledger)
        first_txn_in_this_view_seq_no = get_seq_no(first_txn_in_this_view)
        if first_txn_in_this_view_seq_no <= 1:
            self.node_reg_at_beginning_of_last_view = list(self.node_reg_at_beginning_of_this_view)
            return

        # 4. Get the first audit txn for the last view
        first_txn_in_last_view = self.__get_first_txn_in_view_from_audit(audit_ledger,
                                                                         audit_ledger.getBySeqNo(
                                                                             first_txn_in_this_view_seq_no - 1))
        self.node_reg_at_beginning_of_last_view = list(self.__load_node_reg_for_view(first_txn_in_last_view))

    def __load_node_reg_from_pool_ledger(self, to=None):
        node_reg = []
        for _, txn in self.ledger.getAllTxn(to=to):
            if get_type(txn) != NODE:
                continue
            txn_data = get_payload_data(txn)
            node_name = txn_data[DATA][ALIAS]
            services = txn_data[DATA].get(SERVICES)

            if node_name not in node_reg and VALIDATOR in services:
                # new node added or old one promoted
                node_reg.append(node_name)
            elif node_name in node_name and VALIDATOR not in services:
                # existing node demoted
                node_reg.remove(node_name)
        return node_reg

    # TODO: create a helper class to get data from Audit Ledger
    def __load_current_node_reg_from_audit_ledger(self):
        audit_ledger = self.database_manager.get_ledger(AUDIT_LEDGER_ID)
        if not audit_ledger:
            return None

        last_txn = audit_ledger.get_last_committed_txn()
        last_txn_node_reg = get_payload_data(last_txn).get(AUDIT_TXN_NODE_REG)
        if last_txn_node_reg is None:
            return None

        if isinstance(last_txn_node_reg, int):
            seq_no = get_seq_no(last_txn) - last_txn_node_reg
            audit_txn_for_seq_no = audit_ledger.getBySeqNo(seq_no)
            last_txn_node_reg = get_payload_data(audit_txn_for_seq_no).get(AUDIT_TXN_NODE_REG)

        if last_txn_node_reg is None:
            return None
        return last_txn_node_reg

    def __load_node_reg_for_view(self, audit_txn):
        txn_seq_no = get_seq_no(audit_txn)

        # If this is the first txn in the audit ledger, so that we don't know a full history,
        # then get node reg from the pool ledger
        if txn_seq_no <= 1:
            genesis_pool_ledger_size = get_payload_data(audit_txn)[AUDIT_TXN_LEDGERS_SIZE][POOL_LEDGER_ID] - 1
            return self.__load_node_reg_from_pool_ledger(to=genesis_pool_ledger_size)

        # Get the node reg from audit txn
        node_reg = get_payload_data(audit_txn).get(AUDIT_TXN_NODE_REG)
        if node_reg is None:
            # we don't have node reg in audit ledger yet, so get it from the pool ledger
            pool_ledger_size_for_txn = get_payload_data(audit_txn)[AUDIT_TXN_LEDGERS_SIZE][POOL_LEDGER_ID]
            node_reg = self.__load_node_reg_from_pool_ledger(to=pool_ledger_size_for_txn)
        return node_reg

    def __get_first_txn_in_view_from_audit(self, audit_ledger, audit_txn):
        txn_primaries = get_payload_data(audit_txn).get(AUDIT_TXN_PRIMARIES)
        if isinstance(txn_primaries, int):
            seq_no = get_seq_no(audit_txn) - txn_primaries
            audit_txn = audit_ledger.getBySeqNo(seq_no)
        return audit_txn
