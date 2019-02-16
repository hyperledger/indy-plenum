from plenum.common.ledger import Ledger
from plenum.common.messages.node_messages import BatchCommitted


class ThreePcBatch:
    def __init__(self,
                 ledger_id,
                 inst_id, view_no, pp_seq_no,
                 pp_time,
                 state_root, txn_root) -> None:
        self.ledger_id = ledger_id
        self.inst_id = inst_id
        self.view_no = view_no
        self.pp_seq_no = pp_seq_no
        self.pp_time = pp_time
        self.state_root = state_root
        self.txn_root = txn_root

    @staticmethod
    def from_pre_prepare(pre_prepare, state_root, txn_root):
        return ThreePcBatch(ledger_id=pre_prepare.ledgerId,
                            inst_id=pre_prepare.instId,
                            view_no=pre_prepare.viewNo,
                            pp_seq_no=pre_prepare.ppSeqNo,
                            pp_time=pre_prepare.ppTime,
                            # do not trust PrePrepare's root hashes and use the current replica's ones
                            state_root=state_root,
                            txn_root=txn_root)

    @staticmethod
    def from_ordered(ordered):
        return ThreePcBatch(ledger_id=ordered.ledgerId,
                            inst_id=ordered.instId,
                            view_no=ordered.viewNo,
                            pp_seq_no=ordered.ppSeqNo,
                            pp_time=ordered.ppTime,
                            state_root=Ledger.strToHash(ordered.stateRootHash),
                            txn_root=Ledger.strToHash(ordered.txnRootHash))

    @staticmethod
    def from_batch_committed(batch_comitted: BatchCommitted):
        return ThreePcBatch(ledger_id=batch_comitted.ledgerId,
                            inst_id=batch_comitted.instId,
                            view_no=batch_comitted.viewNo,
                            pp_seq_no=batch_comitted.ppSeqNo,
                            pp_time=batch_comitted.ppTime,
                            state_root=Ledger.strToHash(batch_comitted.stateRootHash),
                            txn_root=Ledger.strToHash(batch_comitted.txnRootHash))
