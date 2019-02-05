from abc import abstractmethod

from common.exceptions import PlenumValueError
from common.serializers.serialization import state_roots_serializer
from plenum.server.database_manager import DatabaseManager


class BatchRequestHandler:
    def __init__(self, database_manager: DatabaseManager, ledger_id):
        self.database_manager = database_manager
        self.ledger_id = ledger_id

    def commit_batch(self, txn_count, state_root, txn_root, pp_time, prev_result):
        """
        :param txn_count: The number of requests to commit (The actual requests
        are picked up from the uncommitted list from the ledger)
        :param state_root: The state trie root after the txns are committed
        :param txn_root: The txn merkle root after the txns are committed

        :return: list of committed transactions
        """

        return self._commit(self.ledger, self.state, txn_count, state_root,
                            txn_root)

    @abstractmethod
    def post_batch_applied(self, state_root):
        pass

    @abstractmethod
    def post_batch_rejected(self):
        pass

    @staticmethod
    def _commit(ledger, state, txn_count, state_root, txn_root):
        _, committedTxns = ledger.commitTxns(txn_count)
        state_root = state_roots_serializer.deserialize(state_root.encode()) if isinstance(
            state_root, str) else state_root
        # TODO test for that
        if ledger.root_hash != txn_root:
            # Probably the following fail should trigger catchup
            # TODO add repr / str for Ledger class and dump it here as well
            raise PlenumValueError(
                'txnRoot', txn_root,
                ("equal to current ledger root hash {}"
                    .format(ledger.root_hash))
            )
        state.commit(rootHash=state_root)
        return committedTxns

    @property
    def state(self):
        return self.database_manager.get_database(self.ledger_id).state \
            if self.ledger_id is not None else None

    @property
    def ledger(self):
        return self.database_manager.get_database(self.ledger_id).ledger \
            if self.ledger_id is not None else None
