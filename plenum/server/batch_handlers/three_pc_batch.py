from plenum.common.ledger import Ledger
from plenum.common.types import f


class ThreePcBatch:
    def __init__(self,
                 ledger_id,
                 inst_id, view_no, pp_seq_no,
                 pp_time,
                 valid_txn_count,
                 state_root, txn_root,
                 has_audit_txn=True) -> None:
        self.ledger_id = ledger_id
        self.inst_id = inst_id
        self.view_no = view_no
        self.pp_seq_no = pp_seq_no
        self.pp_time = pp_time
        self.valid_txn_count = valid_txn_count
        self.state_root = state_root
        self.txn_root = txn_root
        self.has_audit_txn = has_audit_txn

    @staticmethod
    def from_pre_prepare(pre_prepare, valid_txn_count, state_root, txn_root):
        return ThreePcBatch(ledger_id=pre_prepare.ledgerId,
                            inst_id=pre_prepare.instId,
                            view_no=pre_prepare.viewNo,
                            pp_seq_no=pre_prepare.ppSeqNo,
                            pp_time=pre_prepare.ppTime,
                            # do not trust PrePrepare's root hashes and use the current replica's ones
                            valid_txn_count=valid_txn_count,
                            state_root=state_root,
                            txn_root=txn_root,
                            has_audit_txn=f.AUDIT_TXN_ROOT_HASH.nm in pre_prepare and pre_prepare.auditTxnRootHash is not None)

    @staticmethod
    def from_ordered(ordered):
        return ThreePcBatch(ledger_id=ordered.ledgerId,
                            inst_id=ordered.instId,
                            view_no=ordered.viewNo,
                            pp_seq_no=ordered.ppSeqNo,
                            pp_time=ordered.ppTime,
                            valid_txn_count=len(ordered.valid_reqIdr),
                            state_root=Ledger.strToHash(ordered.stateRootHash),
                            txn_root=Ledger.strToHash(ordered.txnRootHash),
                            has_audit_txn=f.AUDIT_TXN_ROOT_HASH.nm in ordered and ordered.auditTxnRootHash is not None)

    @staticmethod
    def from_batch_committed_dict(batch_comitted):
        return ThreePcBatch(ledger_id=batch_comitted[f.LEDGER_ID.nm],
                            inst_id=batch_comitted[f.INST_ID.nm],
                            view_no=batch_comitted[f.VIEW_NO.nm],
                            pp_seq_no=batch_comitted[f.PP_SEQ_NO.nm],
                            pp_time=batch_comitted[f.PP_TIME.nm],
                            valid_txn_count=batch_comitted[f.SEQ_NO_END.nm] - batch_comitted[f.SEQ_NO_START.nm] + 1,
                            state_root=Ledger.strToHash(batch_comitted[f.STATE_ROOT.nm]),
                            txn_root=Ledger.strToHash(batch_comitted[f.TXN_ROOT.nm]),
                            has_audit_txn=f.AUDIT_TXN_ROOT_HASH.nm in batch_comitted and batch_comitted[
                                f.AUDIT_TXN_ROOT_HASH.nm] is not None)
