from plenum.common.constants import POOL_LEDGER_ID, ALIAS, SERVICES, VALIDATOR, NODE, DATA, AUDIT_LEDGER_ID, \
    AUDIT_TXN_NODE_REG, TYPE
from plenum.common.request import Request
from plenum.common.txn_util import get_type, get_payload_data, get_seq_no
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler


class NodeRegBatchHandler(BatchRequestHandler, WriteRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        BatchRequestHandler.__init__(self, database_manager, POOL_LEDGER_ID)
        WriteRequestHandler.__init__(self, database_manager, NODE, POOL_LEDGER_ID)
        self.uncommitted_node_reg = []
        self.committed_node_reg = []
        self.last_view_node_reg = []

    def on_catchup_finished(self):
        node_reg = self.__load_node_reg()
        self.uncommitted_node_reg = list(node_reg)
        self.committed_node_reg = list(node_reg)
        self.last_view_node_reg = list(node_reg)

    def post_batch_applied(self, three_pc_batch, prev_handler_result=None):
        three_pc_batch.node_reg = self.uncommitted_node_reg

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        pass

    def commit_batch(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        pass

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

    def _update_uncommitted_node_reg(self, three_pc_batch: ThreePcBatch):
        if three_pc_batch.ledger_id != POOL_LEDGER_ID:
            return

        audit_ledger = self.database_manager.get_ledger(AUDIT_LEDGER_ID)
        if not audit_ledger:
            return

        audit_ledger.get_last_committed_txn()

    def __load_node_reg(self):
        node_reg = self.__load_node_reg_from_audit_ledger()
        if node_reg is None:
            node_reg = self.__load_node_reg_from_pool_ledger()
        return node_reg

    def __load_node_reg_from_pool_ledger(self):
        node_reg = []
        for _, txn in self.ledger.getAllTxn():
            if get_type(txn) != NODE:
                continue
            txn_data = get_payload_data(txn)
            node_name = txn_data[DATA][ALIAS]
            services = txn_data[DATA].get(SERVICES)
            if isinstance(services, list) and VALIDATOR in services:
                node_reg.append(node_name)
        return node_reg

    def __load_node_reg_from_audit_ledger(self):
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
