from collections import Iterable

from common.exceptions import LogicError
from ledger.ledger import Ledger
from plenum.common.constants import AUDIT_LEDGER_ID, TXN_VERSION, AUDIT_TXN_VIEW_NO, AUDIT_TXN_PP_SEQ_NO, \
    AUDIT_TXN_LEDGERS_SIZE, AUDIT_TXN_LEDGER_ROOT, AUDIT_TXN_STATE_ROOT, AUDIT_TXN_PRIMARIES, AUDIT_TXN_DIGEST, \
    AUDIT_TXN_NODE_REG, CURRENT_TXN_PAYLOAD_VERSIONS, AUDIT, CURRENT_TXN_VERSION
from plenum.common.ledger_uncommitted_tracker import LedgerUncommittedTracker
from plenum.common.transactions import PlenumTransactions
from plenum.common.txn_util import init_empty_txn, set_payload_data, get_payload_data, get_seq_no
from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.database_manager import DatabaseManager
from stp_core.common.log import getlogger

logger = getlogger()


class AuditBatchHandler(BatchRequestHandler):

    def __init__(self, database_manager: DatabaseManager, ):
        super().__init__(database_manager, AUDIT_LEDGER_ID)
        # TODO: move it to BatchRequestHandler
        self.tracker = LedgerUncommittedTracker(None, self.ledger.uncommitted_root_hash, self.ledger.size)

    def post_batch_applied(self, three_pc_batch: ThreePcBatch, prev_handler_result=None):
        txn = self._add_to_ledger(three_pc_batch)
        self.tracker.apply_batch(None, self.ledger.uncommitted_root_hash, self.ledger.uncommitted_size)
        logger.debug("applied audit txn {}; uncommitted root hash is {}; uncommitted size is {}".
                     format(str(txn), self.ledger.uncommitted_root_hash, self.ledger.uncommitted_size))

    def post_batch_rejected(self, ledger_id, prev_handler_result=None):
        _, _, txn_count = self.tracker.reject_batch()
        self.ledger.discardTxns(txn_count)
        logger.debug("rejected {} audit txns; uncommitted root hash is {}; uncommitted size is {}".
                     format(txn_count, self.ledger.uncommitted_root_hash, self.ledger.uncommitted_size))

    def commit_batch(self, three_pc_batch, prev_handler_result=None):
        _, _, txns_count = self.tracker.commit_batch()
        _, committedTxns = self.ledger.commitTxns(txns_count)
        logger.debug("committed {} audit txns; uncommitted root hash is {}; uncommitted size is {}".
                     format(txns_count, self.ledger.uncommitted_root_hash, self.ledger.uncommitted_size))
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
            logger.info("Has 3PC batch without audit ledger: {}".format(str(three_pc_batch)))
            return

        # 1. prepare AUDIT txn
        txn_data = self._create_audit_txn_data(three_pc_batch, self.ledger.get_last_txn())
        txn = init_empty_txn(txn_type=PlenumTransactions.AUDIT.value)
        txn = set_payload_data(txn, txn_data)

        # 2. Append txn metadata
        self.ledger.append_txns_metadata([txn], three_pc_batch.pp_time)

        # 3. Add to the Ledger
        self.ledger.appendTxns([txn])
        return txn

    def _create_audit_txn_data(self, three_pc_batch, last_audit_txn):
        # 1. general format and (view_no, pp_seq_no)
        view_no = three_pc_batch.original_view_no if three_pc_batch.original_view_no is not None else three_pc_batch.view_no
        txn = {
            TXN_VERSION: CURRENT_TXN_PAYLOAD_VERSIONS[AUDIT],
            AUDIT_TXN_VIEW_NO: view_no,
            AUDIT_TXN_PP_SEQ_NO: three_pc_batch.pp_seq_no,
            AUDIT_TXN_LEDGERS_SIZE: {},
            AUDIT_TXN_LEDGER_ROOT: {},
            AUDIT_TXN_STATE_ROOT: {},
            AUDIT_TXN_PRIMARIES: None,
            AUDIT_TXN_DIGEST: three_pc_batch.pp_digest
        }

        for lid, ledger in self.database_manager.ledgers.items():
            if lid == AUDIT_LEDGER_ID:
                continue
            # 2. ledger size
            txn[AUDIT_TXN_LEDGERS_SIZE][lid] = ledger.uncommitted_size

            # 3. ledger root (either root_hash or seq_no to last changed)
            # TODO: support setting for multiple ledgers
            self._fill_ledger_root_hash(txn, lid, ledger, last_audit_txn, three_pc_batch)

        # 5. set primaries field
        self._fill_primaries(txn, three_pc_batch, last_audit_txn)

        # 6. set nodeReg field
        self._fill_node_reg(txn, three_pc_batch, last_audit_txn)

        return txn

    def _fill_ledger_root_hash(self, txn, lid, ledger, last_audit_txn, three_pc_batch):
        last_audit_txn_data = get_payload_data(last_audit_txn) if last_audit_txn is not None else None

        if lid == three_pc_batch.ledger_id:
            txn[AUDIT_TXN_LEDGER_ROOT][lid] = Ledger.hashToStr(ledger.uncommitted_root_hash)
            txn[AUDIT_TXN_STATE_ROOT][lid] = Ledger.hashToStr(self.database_manager.get_state(lid).headHash)

        # 1. it is the first batch and we have something
        elif last_audit_txn_data is None and ledger.uncommitted_size:
            txn[AUDIT_TXN_LEDGER_ROOT][lid] = Ledger.hashToStr(ledger.uncommitted_root_hash)
            txn[AUDIT_TXN_STATE_ROOT][lid] = Ledger.hashToStr(self.database_manager.get_state(lid).headHash)

        # 1.1. Rare case -- we have previous audit txns but don't have this ledger i.e. new plugins
        elif last_audit_txn_data is not None and last_audit_txn_data[AUDIT_TXN_LEDGERS_SIZE].get(lid, None) is None and \
                len(ledger.uncommittedTxns):
            txn[AUDIT_TXN_LEDGER_ROOT][lid] = Ledger.hashToStr(ledger.uncommitted_root_hash)
            txn[AUDIT_TXN_STATE_ROOT][lid] = Ledger.hashToStr(self.database_manager.get_state(lid).headHash)

        # 2. Usual case -- this ledger was updated since the last audit txn
        elif last_audit_txn_data is not None and last_audit_txn_data[AUDIT_TXN_LEDGERS_SIZE].get(lid,
                                                                                                 None) is not None and \
                ledger.uncommitted_size > last_audit_txn_data[AUDIT_TXN_LEDGERS_SIZE][lid]:
            txn[AUDIT_TXN_LEDGER_ROOT][lid] = Ledger.hashToStr(ledger.uncommitted_root_hash)
            txn[AUDIT_TXN_STATE_ROOT][lid] = Ledger.hashToStr(self.database_manager.get_state(lid).headHash)

        # 3. This ledger is never audited, so do not add the key
        elif last_audit_txn_data is None or lid not in last_audit_txn_data[AUDIT_TXN_LEDGER_ROOT]:
            return

        # 4. ledger is not changed in last batch => delta = delta + 1
        elif isinstance(last_audit_txn_data[AUDIT_TXN_LEDGER_ROOT][lid], int):
            txn[AUDIT_TXN_LEDGER_ROOT][lid] = last_audit_txn_data[AUDIT_TXN_LEDGER_ROOT][lid] + 1

        # 5. ledger is changed in last batch but not changed now => delta = 1
        elif last_audit_txn_data:
            txn[AUDIT_TXN_LEDGER_ROOT][lid] = 1

    def _fill_primaries(self, txn, three_pc_batch, last_audit_txn):
        last_audit_txn_data = get_payload_data(last_audit_txn) if last_audit_txn is not None else None
        last_txn_value = last_audit_txn_data[AUDIT_TXN_PRIMARIES] if last_audit_txn_data else None
        current_primaries = three_pc_batch.primaries

        # 1. First audit txn
        if last_audit_txn_data is None:
            txn[AUDIT_TXN_PRIMARIES] = current_primaries

        # 2. Previous primaries field contains primary list
        # If primaries did not changed, we will store seq_no delta
        # between current txn and last persisted primaries, i.e.
        # we can find seq_no of last actual primaries, like:
        # last_audit_txn_seq_no - last_audit_txn[AUDIT_TXN_PRIMARIES]
        elif isinstance(last_txn_value, Iterable):
            if last_txn_value == current_primaries:
                txn[AUDIT_TXN_PRIMARIES] = 1
            else:
                txn[AUDIT_TXN_PRIMARIES] = current_primaries

        # 3. Previous primaries field is delta
        elif isinstance(last_txn_value, int) and last_txn_value < self.ledger.uncommitted_size:
            last_primaries_seq_no = get_seq_no(last_audit_txn) - last_txn_value
            last_primaries = get_payload_data(
                self.ledger.get_by_seq_no_uncommitted(last_primaries_seq_no))[AUDIT_TXN_PRIMARIES]
            if isinstance(last_primaries, Iterable):
                if last_primaries == current_primaries:
                    txn[AUDIT_TXN_PRIMARIES] = last_txn_value + 1
                else:
                    txn[AUDIT_TXN_PRIMARIES] = current_primaries
            else:
                raise LogicError('Value, mentioned in primaries field must be a '
                                 'seq_no of a txn with primaries')

        # 4. That cannot be
        else:
            raise LogicError('Incorrect primaries field in audit ledger (seq_no: {}. value: {})'.format(
                get_seq_no(last_audit_txn), last_txn_value))

    def _fill_node_reg(self, txn, three_pc_batch, last_audit_txn):
        last_audit_txn_data = get_payload_data(last_audit_txn) if last_audit_txn is not None else None
        last_audit_node_reg = last_audit_txn_data.get(AUDIT_TXN_NODE_REG) if last_audit_txn_data else None
        current_node_reg = three_pc_batch.node_reg

        if current_node_reg is None:
            return

        # 1. First audit txn with node reg
        if last_audit_node_reg is None:
            txn[AUDIT_TXN_NODE_REG] = current_node_reg

        # 2. Previous nodeReg field contains nodeReg list
        # If nodeReg did not changed, we will store seq_no delta
        # between current txn and last persisted nodeReg, i.e.
        # we can find seq_no of last actual nodeReg, like:
        # last_audit_txn_seq_no - last_audit_txn[AUDIT_TXN_NODE_REG]
        elif isinstance(last_audit_node_reg, Iterable):
            if last_audit_node_reg == current_node_reg:
                txn[AUDIT_TXN_NODE_REG] = 1
            else:
                txn[AUDIT_TXN_NODE_REG] = current_node_reg

        # 3. Previous nodeReg field is delta
        elif isinstance(last_audit_node_reg, int) and last_audit_node_reg < self.ledger.uncommitted_size:
            last_node_reg_seq_no = get_seq_no(last_audit_txn) - last_audit_node_reg
            last_node_reg = get_payload_data(
                self.ledger.get_by_seq_no_uncommitted(last_node_reg_seq_no))[AUDIT_TXN_NODE_REG]
            if isinstance(last_node_reg, Iterable):
                if last_node_reg == current_node_reg:
                    txn[AUDIT_TXN_NODE_REG] = last_audit_node_reg + 1
                else:
                    txn[AUDIT_TXN_NODE_REG] = current_node_reg
            else:
                raise LogicError('Value, mentioned in nodeReg field must be a '
                                 'seq_no of a txn with nodeReg')
