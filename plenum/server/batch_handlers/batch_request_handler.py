from abc import abstractmethod

from common.exceptions import PlenumValueError
from common.serializers.serialization import state_roots_serializer
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.database_manager import DatabaseManager


class BatchRequestHandler:
    def __init__(self, database_manager: DatabaseManager, ledger_id):
        self.database_manager = database_manager
        self.ledger_id = ledger_id

    def commit_batch(self, three_pc_batch, prev_handler_result=None):
        """
        :param txn_count: The number of requests to commit (The actual requests
        are picked up from the uncommitted list from the ledger)
        :param state_root: The state trie root after the txns are committed
        :param txn_root: The txn merkle root after the txns are committed

        :return: list of committed transactions
        """

        return self._commit(self.ledger, self.state, three_pc_batch)

    @abstractmethod
    def post_batch_applied(self, three_pc_batch, prev_handler_result=None):
        pass

    @abstractmethod
    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        pass

    @staticmethod
    def _commit(ledger, state, three_pc_batch: ThreePcBatch):
        _, committedTxns = ledger.commitTxns(len(three_pc_batch.valid_digests))
        state_root = state_roots_serializer.deserialize(three_pc_batch.state_root.encode()) if isinstance(
            three_pc_batch.state_root, str) else three_pc_batch.state_root
        # TODO test for that
        if ledger.root_hash != three_pc_batch.txn_root:
            # Probably the following fail should trigger catchup
            # TODO add repr / str for Ledger class and dump it here as well
            raise PlenumValueError(
                'txnRoot', three_pc_batch.txn_root,
                ("equal to current ledger root hash {}"
                    .format(ledger.root_hash))
            )
        state.commit(rootHash=state_root)
        return committedTxns

    def _check_consistency_after_commit(self, txn_root, state_root=None):
        if self.ledger.root_hash != txn_root:
            raise PlenumValueError(
                'txnRoot', txn_root,
                ("equal to current ledger root hash {}"
                    .format(self.ledger.root_hash))
            )
        if state_root is not None and self.state is not None:
            if self.state.committedHeadHash != state_root:
                raise PlenumValueError(
                    'stateRoot', state_root,
                    ("equal to current state root hash {}"
                        .format(self.state.committedHeadHash))
                )

    @property
    def state(self):
        return self.database_manager.get_database(self.ledger_id).state \
            if self.ledger_id is not None else None

    @property
    def ledger(self):
        return self.database_manager.get_database(self.ledger_id).ledger \
            if self.ledger_id is not None else None
