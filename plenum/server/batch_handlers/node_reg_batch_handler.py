from collections import OrderedDict

from plenum.common.constants import POOL_LEDGER_ID, NODE
from plenum.common.txn_util import get_type, get_payload_data
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.database_manager import DatabaseManager


class NodeRegBatchHandler(BatchRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, POOL_LEDGER_ID)
        self.uncommitted_node_reg = []
        self.committed_node_reg = []
        self.last_view_node_reg = []

    def on_catchup_finished(self):
        self.__init_node_reg()

    def post_batch_applied(self, three_pc_batch, prev_handler_result=None):
        three_pc_batch.node_reg = self.uncommitted_node_reg

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        pass

    def commit_batch(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        pass

    def __init_node_reg(self):
        pass
        # self._ordered_node_ids = OrderedDict()
        # self._ordered_node_services = {}
        # for _, txn in self.ledger.getAllTxn():
        #     if get_type(txn) == NODE:
        #         txn_data = get_payload_data(txn)
        #         self._ordered_node_ids[node_nym] = node_name
        #         self._set_node_ids_in_cache(txn_data[TARGET_NYM],
        #                                     txn_data[DATA][ALIAS])
        #         self._set_node_services_in_cache(txn_data[TARGET_NYM],
        #                                          txn_data[DATA].get(SERVICES, None))
