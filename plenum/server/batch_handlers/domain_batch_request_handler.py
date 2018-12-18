from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.database_manager import DatabaseManager


class DomainBatchHandler(BatchRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, DOMAIN_LEDGER_ID)

    def post_apply_batch(self):
        pass

    def commit_batch(self, txnCount, stateRoot, txnRoot, ppTime):
        self._commit(self.ledger, self.state, txnCount, stateRoot, txnRoot)
        self.ts_store.set(ppTime, stateRoot)

    def revert_batch(self):
        pass

    @property
    def ts_store(self):
        return self.database_manager.get_store('ts')
