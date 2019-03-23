from ledger.ledger import Ledger
from plenum.common.constants import AUDIT_LEDGER_ID, TXN_VERSION, AUDIT_TXN_VIEW_NO, AUDIT_TXN_PP_SEQ_NO, \
    AUDIT_TXN_LEDGERS_SIZE, AUDIT_TXN_LEDGER_ROOT, AUDIT_TXN_STATE_ROOT
from plenum.common.ledger_uncommitted_tracker import LedgerUncommittedTracker
from plenum.common.transactions import PlenumTransactions
from plenum.common.txn_util import init_empty_txn, set_payload_data, get_payload_data, get_seq_no
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.database_manager import DatabaseManager


class AuditBatchHandler(BatchRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, AUDIT_LEDGER_ID)
        # TODO: move it to BatchRequestHandler
        self.tracker = LedgerUncommittedTracker(None, self.ledger.uncommitted_root_hash, self.ledger.size)

    def post_batch_applied(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        self._add_to_ledger(three_pc_batch)
        self.tracker.apply_batch(None, self.ledger.uncommitted_root_hash, self.ledger.uncommitted_size)

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        _, _, txn_count = self.tracker.reject_batch()
        self.ledger.discardTxns(txn_count)

    def commit_batch(self, ledger_id, txn_count, state_root, txn_root, pp_time, prev_handler_result=None):
        _, _, txns_count = self.tracker.commit_batch()
        _, committedTxns = self.ledger.commitTxns(txns_count)
        return committedTxns

    def on_catchup_finished(self):
        self.tracker.set_last_committed(state_root=None,
                                        txn_root=self.ledger.uncommitted_root_hash,
                                        ledger_size=self.ledger.size)

    @staticmethod
    def transform_txn_for_ledger(txn):
        '''
        Makes sure that we have integer as keys after possible deserialization from json
        :param txn: txn to be transformed
        :return: transformed txn
        '''
        txn_data = get_payload_data(txn)
        txn_data[AUDIT_TXN_LEDGERS_SIZE] = {int(k): v for k, v in txn_data[AUDIT_TXN_LEDGERS_SIZE].items()}
        txn_data[AUDIT_TXN_LEDGER_ROOT] = {int(k): v for k, v in txn_data[AUDIT_TXN_LEDGER_ROOT].items()}
        txn_data[AUDIT_TXN_STATE_ROOT] = {int(k): v for k, v in txn_data[AUDIT_TXN_STATE_ROOT].items()}
        return txn

    def _add_to_ledger(self, three_pc_batch: ThreePcBatch):
        # if PRE-PREPARE doesn't have audit txn (probably old code) - do nothing
        # TODO: remove this check after all nodes support audit ledger
        if not three_pc_batch.has_audit_txn:
            return

        # 1. prepare AUDIT txn
        txn_data = self._create_audit_txn_data(three_pc_batch, self.ledger.get_last_txn())
        txn = init_empty_txn(txn_type=PlenumTransactions.AUDIT.value)
        txn = set_payload_data(txn, txn_data)

        # 2. Append txn metadata
        self.ledger.append_txns_metadata([txn], three_pc_batch.pp_time)

        # 3. Add to the Ledger
        self.ledger.appendTxns([txn])

    def _create_audit_txn_data(self, three_pc_batch, last_audit_txn):
        # 1. general format and (view_no, pp_seq_no)
        txn = {
            TXN_VERSION: "1",
            AUDIT_TXN_VIEW_NO: three_pc_batch.view_no,
            AUDIT_TXN_PP_SEQ_NO: three_pc_batch.pp_seq_no,
            AUDIT_TXN_LEDGERS_SIZE: {},
            AUDIT_TXN_LEDGER_ROOT: {},
            AUDIT_TXN_STATE_ROOT: {}
        }

        for lid, ledger in self.database_manager.ledgers.items():
            if lid == AUDIT_LEDGER_ID:
                continue
            # 2. ledger size
            txn[AUDIT_TXN_LEDGERS_SIZE][lid] = ledger.uncommitted_size

            # 3. ledger root (either root_hash or seq_no to last changed)
            # TODO: support setting for multiple ledgers
            self.__fill_ledger_root_hash(txn, three_pc_batch, lid, last_audit_txn)

        # 4. state root hash
        txn[AUDIT_TXN_STATE_ROOT][three_pc_batch.ledger_id] = Ledger.hashToStr(three_pc_batch.state_root)

        return txn

    def __fill_ledger_root_hash(self, txn, three_pc_batch, lid, last_audit_txn):
        target_ledger_id = three_pc_batch.ledger_id
        last_audit_txn_data = get_payload_data(last_audit_txn) if last_audit_txn is not None else None

        # 1. ledger is changed in this batch => root_hash
        if lid == target_ledger_id:
            txn[AUDIT_TXN_LEDGER_ROOT][lid] = Ledger.hashToStr(three_pc_batch.txn_root)

        # 2. This ledger is never audited, so do not add the key
        elif last_audit_txn_data is None or lid not in last_audit_txn_data[AUDIT_TXN_LEDGER_ROOT]:
            return

        # 3. ledger is not changed in last batch => delta = delta + 1
        elif isinstance(last_audit_txn_data[AUDIT_TXN_LEDGER_ROOT][lid], int):
            txn[AUDIT_TXN_LEDGER_ROOT][lid] = last_audit_txn_data[AUDIT_TXN_LEDGER_ROOT][lid] + 1

        # 4. ledger is changed in last batch but not changed now => delta = 1
        elif last_audit_txn_data:
            txn[AUDIT_TXN_LEDGER_ROOT][lid] = 1
