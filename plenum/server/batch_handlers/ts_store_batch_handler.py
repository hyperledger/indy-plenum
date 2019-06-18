from common.serializers.serialization import state_roots_serializer
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.database_manager import DatabaseManager


class TsStoreBatchHandler(BatchRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, None)

    def commit_batch(self, three_pc_batch, prev_handler_result=None):
        """
        :param txn_count: The number of requests to commit (The actual requests
        are picked up from the uncommitted list from the ledger)
        :param state_root: The state trie root after the txns are committed
        :param txn_root: The txn merkle root after the txns are committed

        :return: list of committed transactions
        """
        state_root = state_roots_serializer.deserialize(three_pc_batch.state_root.encode()) \
            if isinstance(three_pc_batch.state_root, str) else three_pc_batch.state_root
        self.database_manager.ts_store.set(three_pc_batch.pp_time,
                                           state_root,
                                           three_pc_batch.ledger_id)

    def post_batch_applied(self, three_pc_batch, prev_handler_result=None):
        pass

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        pass
