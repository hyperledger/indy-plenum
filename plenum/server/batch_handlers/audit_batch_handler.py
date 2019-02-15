from ledger.ledger import Ledger
from plenum.common.constants import AUDIT_LEDGER_ID, TXN_VERSION, AUDIT_TXN_VIEW_NO, AUDIT_TXN_PP_SEQ_NO, \
    AUDIT_TXN_LEDGERS_SIZE, AUDIT_TXN_LEDGER_ROOT, AUDIT_TXN_STATE_ROOT
from plenum.common.transactions import PlenumTransactions
from plenum.common.txn_util import init_empty_txn, set_payload_data, get_payload_data, get_seq_no
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.database_manager import DatabaseManager


class AuditBatchHandler(BatchRequestHandler):

    def __init__(self, database_manager: DatabaseManager, master_replica):
        super().__init__(database_manager, AUDIT_LEDGER_ID)
        self._master_replica = master_replica

    def _create_audit_txn_data(self, ledger_id, last_audit_txn):
        # 1. general format and (view_no, pp_seq_no)
        txn = {
            TXN_VERSION: "1",
            AUDIT_TXN_VIEW_NO: self._master_replica.viewNo,
            AUDIT_TXN_PP_SEQ_NO: self._master_replica.lastPrePrepareSeqNo,
            AUDIT_TXN_LEDGERS_SIZE: {},
            AUDIT_TXN_LEDGER_ROOT: {},
            AUDIT_TXN_STATE_ROOT: {}
        }

        for lid, ledger in self.database_manager.ledgers.items():
            if lid == AUDIT_LEDGER_ID:
                continue
            # 2. ledger size
            txn[AUDIT_TXN_LEDGERS_SIZE][str(lid)] = ledger.uncommitted_size

            # 3. ledger root (either root_hash or seq_no to last changed)
            # TODO: support setting for multiple ledgers
            self.__fill_ledger_root_hash(txn, ledger_id, lid, last_audit_txn)

        # 4. state root hash
        txn[AUDIT_TXN_STATE_ROOT][str(ledger_id)] = Ledger.hashToStr(
            self.database_manager.get_state(ledger_id).headHash
        )

        return txn

    def __fill_ledger_root_hash(self, txn, target_ledger_id, lid, last_audit_txn):
        last_audit_txn_data = get_payload_data(last_audit_txn) if last_audit_txn is not None else None

        # 1. ledger is changed in this batch => root_hash
        if lid == target_ledger_id:
            ledger = self.database_manager.get_ledger(target_ledger_id)
            root_hash = ledger.uncommittedRootHash or ledger.tree.root_hash
            txn[AUDIT_TXN_LEDGER_ROOT][str(lid)] = Ledger.hashToStr(root_hash)

        # 2. This ledger is never audited, so do not add the key
        elif last_audit_txn_data is None or str(lid) not in last_audit_txn_data[AUDIT_TXN_LEDGER_ROOT]:
            return

        # 3. ledger is not changed in last batch => the same audit seq no
        elif isinstance(last_audit_txn_data[AUDIT_TXN_LEDGER_ROOT][str(lid)], int):
            txn[AUDIT_TXN_LEDGER_ROOT][str(lid)] = last_audit_txn_data[AUDIT_TXN_LEDGER_ROOT][str(lid)]

        # 4. ledger is changed in last batch but not changed now => seq_no of last audit txn
        elif last_audit_txn_data:
            txn[AUDIT_TXN_LEDGER_ROOT][str(lid)] = get_seq_no(last_audit_txn)

    def post_batch_applied(self, ledger_id, state_root, pp_time, prev_result=None):
        # 1. prepare AUDIT txn
        txn_data = self._create_audit_txn_data(ledger_id, self.ledger.get_last_txn())
        txn = init_empty_txn(txn_type=PlenumTransactions.AUDIT.value)
        txn = set_payload_data(txn, txn_data)

        # 2. Append txn metadata
        self.ledger.append_txns_metadata([txn], pp_time)

        # 3. Add to the Ledger
        self.ledger.appendTxns([txn])

    def post_batch_rejected(self, ledger_id):
        # Audit ledger always has 1 txn per 3PC batch
        self.ledger.discardTxns(1)

    def commit_batch(self, ledger_id, txn_count, state_root, txn_root, pp_time, prev_result=None):
        # Audit ledger always has 1 txn per 3PC batch
        _, committedTxns = self.ledger.commitTxns(1)
        return committedTxns
