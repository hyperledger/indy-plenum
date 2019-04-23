from plenum.common.ledger import Ledger
from plenum.common.request import Request
from plenum.common.types import f


class ThreePcBatch:
    def __init__(self,
                 ledger_id,
                 inst_id, view_no, pp_seq_no,
                 pp_time,
                 state_root, txn_root,
                 primaries,
                 valid_digests,
                 has_audit_txn=True) -> None:
        self.ledger_id = ledger_id
        self.inst_id = inst_id
        self.view_no = view_no
        self.pp_seq_no = pp_seq_no
        self.pp_time = pp_time
        self.state_root = state_root
        self.txn_root = txn_root
        self.primaries = primaries
        self.valid_digests = valid_digests
        self.has_audit_txn = has_audit_txn

    def __repr__(self) -> str:
        return str(self.__dict__)

    @staticmethod
    def from_pre_prepare(pre_prepare, state_root, txn_root, primaries, valid_digests):
        return ThreePcBatch(ledger_id=pre_prepare.ledgerId,
                            inst_id=pre_prepare.instId,
                            view_no=pre_prepare.viewNo,
                            pp_seq_no=pre_prepare.ppSeqNo,
                            pp_time=pre_prepare.ppTime,
                            # do not trust PrePrepare's root hashes and use the current replica's ones
                            state_root=state_root,
                            txn_root=txn_root,
                            primaries=primaries,
                            valid_digests=valid_digests,
                            has_audit_txn=f.AUDIT_TXN_ROOT_HASH.nm in pre_prepare and pre_prepare.auditTxnRootHash is not None)

    @staticmethod
    def from_ordered(ordered):
        return ThreePcBatch(ledger_id=ordered.ledgerId,
                            inst_id=ordered.instId,
                            view_no=ordered.viewNo,
                            pp_seq_no=ordered.ppSeqNo,
                            pp_time=ordered.ppTime,
                            state_root=Ledger.strToHash(ordered.stateRootHash),
                            txn_root=Ledger.strToHash(ordered.txnRootHash),
                            primaries=ordered.primaries,
                            valid_digests=ordered.valid_reqIdr,
                            has_audit_txn=f.AUDIT_TXN_ROOT_HASH.nm in ordered and ordered.auditTxnRootHash is not None)

    @staticmethod
    def from_batch_committed_dict(batch_comitted):
        valid_req_keys = [Request(**req_dict).key for req_dict in batch_comitted[f.REQUESTS.nm]]
        return ThreePcBatch(ledger_id=batch_comitted[f.LEDGER_ID.nm],
                            inst_id=batch_comitted[f.INST_ID.nm],
                            view_no=batch_comitted[f.VIEW_NO.nm],
                            pp_seq_no=batch_comitted[f.PP_SEQ_NO.nm],
                            pp_time=batch_comitted[f.PP_TIME.nm],
                            state_root=Ledger.strToHash(batch_comitted[f.STATE_ROOT.nm]),
                            txn_root=Ledger.strToHash(batch_comitted[f.TXN_ROOT.nm]),
                            primaries=batch_comitted[f.PRIMARIES.nm],
                            valid_digests=valid_req_keys,
                            has_audit_txn=f.AUDIT_TXN_ROOT_HASH.nm in batch_comitted and batch_comitted[
                                f.AUDIT_TXN_ROOT_HASH.nm] is not None)
