from abc import abstractmethod

from common.exceptions import PlenumValueError
from common.serializers.serialization import state_roots_serializer
from plenum.server.database_manager import DatabaseManager


class BatchRequestHandler:
    def __init__(self, database_manager: DatabaseManager, ledger_id):
        self.database_manager = database_manager
        self.ledger_id = ledger_id

    @abstractmethod
    def post_apply_batch(self, state_root):
        pass

    def commit_batch(self, txnCount, stateRoot, txnRoot, ppTime):
        """
        :param txnCount: The number of requests to commit (The actual requests
        are picked up from the uncommitted list from the ledger)
        :param stateRoot: The state trie root after the txns are committed
        :param txnRoot: The txn merkle root after the txns are committed

        :return: list of committed transactions
        """

        return self._commit(self.ledger, self.state, txnCount, stateRoot,
                            txnRoot)

    @abstractmethod
    def revert_batch(self):
        pass

    @staticmethod
    def _commit(ledger, state, txnCount, stateRoot, txnRoot):
        _, committedTxns = ledger.commitTxns(txnCount)
        stateRoot = state_roots_serializer.deserialize(stateRoot.encode()) if isinstance(
            stateRoot, str) else stateRoot
        # TODO test for that
        if ledger.root_hash != txnRoot:
            # Probably the following fail should trigger catchup
            # TODO add repr / str for Ledger class and dump it here as well
            raise PlenumValueError(
                'txnRoot', txnRoot,
                ("equal to current ledger root hash {}"
                    .format(ledger.root_hash))
            )
        state.commit(rootHash=stateRoot)
        return committedTxns

    @property
    def state(self):
        return self.database_manager.get_database(self.ledger_id).state

    @property
    def ledger(self):
        return self.database_manager.get_database(self.ledger_id).ledger
