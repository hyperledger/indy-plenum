from typing import Optional

from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.ledger import Ledger
from plenum.common.messages.node_messages import LedgerStatus, ConsistencyProof, CatchupReq, CatchupRep
from plenum.common.util import SortedDict
from stp_core.common.log import getlogger

logger = getlogger()


class CatchupLedgerDataProvider:
    def __init__(self, name: str, ledger_id: int, ledger: Ledger):
        self._name = name
        self._ledger_id = ledger_id
        self._ledger = ledger

    def __repr__(self):
        return "{}, ledger id {}".format(self._name, self._ledger_id)

    def get_ledger_status(self) -> LedgerStatus:
        return LedgerStatus(self._ledger_id,
                            self._ledger.size,
                            None,
                            None,
                            self._ledger.root_hash,
                            CURRENT_PROTOCOL_VERSION)

    def get_consistency_proof(self, seq_no_start: int, seq_no_end: int) -> Optional[ConsistencyProof]:
        if seq_no_end < seq_no_start:
            logger.error("{} cannot build consistency proof since end {} is less than start {}".
                         format(self, seq_no_end, seq_no_start))
            return

        if seq_no_start > self._ledger.size:
            logger.error("{} cannot build consistency proof from {} since its ledger size is {}"
                         .format(self, seq_no_start, self._ledger.size))
            return

        if seq_no_end > self._ledger.size:
            logger.error("{} cannot build consistency proof till {} since its ledger size is {}"
                         .format(self, seq_no_end, self._ledger.size))
            return

        if seq_no_start == 0:
            # Consistency proof for an empty tree cannot exist. Using the root
            # hash now so that the node which is behind can verify that
            # TODO: Make this an empty list
            old_root = self._ledger.tree.root_hash
            proof = [old_root, ]
        else:
            proof = self._ledger.tree.consistency_proof(seq_no_start, seq_no_end)
            old_root = self._ledger.tree.merkle_tree_hash(0, seq_no_start)

        new_root = self._ledger.tree.merkle_tree_hash(0, seq_no_end)

        old_root = Ledger.hashToStr(old_root)
        new_root = Ledger.hashToStr(new_root)
        proof = [Ledger.hashToStr(p) for p in proof]

        return ConsistencyProof(self._ledger_id,
                                seq_no_start,
                                seq_no_end,
                                None,
                                None,
                                old_root,
                                new_root,
                                proof)

    def process_ledger_status(self, status: LedgerStatus, frm: str) -> Optional[ConsistencyProof]:
        if status.ledgerId != self._ledger_id:
            raise ValueError("{} received {} for wrong ledger".format(self, status))

        if status.txnSeqNo < 0:
            logger.warning("{} discarding message {} from {} because it contains negative sequence number".
                           format(self, status, frm))
            return

        if status.txnSeqNo >= self._ledger.size:
            return

        return self.get_consistency_proof(status.txnSeqNo, self._ledger.size)

    def process_catchup_req(self, req: CatchupReq, frm: str) -> Optional[CatchupRep]:
        if req.ledgerId != self._ledger_id:
            raise ValueError("{} received {} for wrong ledger".format(self, status))

        start = req.seqNoStart
        end = req.seqNoEnd

        if start > end:
            logger.debug("{} discarding message {} from {} because its start greater than end".
                         format(self, req, frm))
            return

        if end > req.catchupTill:
            logger.debug("{} discarding message {} from {} because its end greater than catchup till".
                         format(self, req, frm))
            return

        if req.catchupTill > self._ledger.size:
            logger.debug("{} discarding message {} from {} because its catchup till greater than ledger size {}".
                         format(self, req, frm, self._ledger.size))
            return

        cons_proof = self._ledger.tree.consistency_proof(end, req.catchupTill)
        cons_proof = [Ledger.hashToStr(p) for p in cons_proof]

        txns = {}
        for seq_no, txn in self._ledger.getAllTxn(start, end):
            # TODO: txns[seq_no] = self.owner.update_txn_with_extra_data(txn)
            txns[seq_no] = txn

        # TODO: Do we really need them sorted?
        sorted_txns = SortedDict(txns)
        return CatchupRep(self._ledger_id,
                          sorted_txns,
                          cons_proof)
