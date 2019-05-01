from abc import abstractmethod
from typing import Any, Tuple, Optional

from plenum.common.channel import RxChannel, Router
from plenum.common.ledger import Ledger
from plenum.common.messages.node_messages import CatchupReq, CatchupRep, ConsistencyProof, LedgerStatus
from plenum.common.util import SortedDict
from plenum.server.catchup.utils import CatchupDataProvider, build_ledger_status
from stp_core.common.log import getlogger

logger = getlogger()


class SeederService:
    def __init__(self, input: RxChannel, provider: CatchupDataProvider):
        router = Router(input)
        router.add(LedgerStatus, self.process_ledger_status)
        router.add(CatchupReq, self.process_catchup_req)
        self._provider = provider

    def __repr__(self):
        return self._provider.node_name()

    def process_ledger_status(self, status: LedgerStatus, frm: str):
        logger.info("{} received ledger status: {} from {}".format(self, status, frm))

        ledger_id, ledger = self._get_ledger_and_id(status)

        if ledger is None:
            self._provider.discard(status, reason="it references invalid ledger",
                                   logMethod=logger.warning)
            return

        if status.txnSeqNo < 0:
            self._provider.discard(status,
                                   reason="Received negative sequence number from {}".format(frm),
                                   logMethod=logger.warning)
            return

        if status.txnSeqNo >= ledger.size:
            self._on_ledger_status_up_to_date(ledger_id, frm)
            return

        cons_proof = self._build_consistency_proof(ledger_id, status.txnSeqNo, ledger.size)
        if cons_proof:
            logger.info("{} sending consistency proof: {} to {}".format(self, cons_proof, frm))
            self._provider.send_to(cons_proof, frm)

    def process_catchup_req(self, req: CatchupReq, frm: str):
        logger.info("{} received catchup request: {} from {}".format(self, req, frm))

        ledger_id, ledger = self._get_ledger_and_id(req)

        if ledger is None:
            self._provider.discard(req, reason="it references invalid ledger", logMethod=logger.warning)
            return

        start = req.seqNoStart
        end = req.seqNoEnd

        if start < 1:
            self._provider.discard(req,
                                   reason="not able to service since start {} is zero or less".
                                   format(start), logMethod=logger.debug)
            return

        if start > end:
            self._provider.discard(req,
                                   reason="not able to service since start = {} greater than end = {}"
                                   .format(start, end), logMethod=logger.debug)
            return

        if end > req.catchupTill:
            self._provider.discard(req,
                                   reason="not able to service since end = {} greater than catchupTill = {}"
                                   .format(end, req.catchupTill), logMethod=logger.debug)
            return

        if req.catchupTill > ledger.size:
            self._provider.discard(req,
                                   reason="not able to service since catchupTill = {} greater than ledger size = {}"
                                   .format(req.catchupTill, ledger.size), logMethod=logger.debug)
            return

        cons_proof = ledger.tree.consistency_proof(end, req.catchupTill)
        cons_proof = [Ledger.hashToStr(p) for p in cons_proof]

        txns = {}
        for seq_no, txn in ledger.getAllTxn(start, end):
            txns[seq_no] = self._provider.update_txn_with_extra_data(txn)

        txns = SortedDict(txns)  # TODO: Do we really need them sorted on the sending side?
        rep = CatchupRep(ledger_id, txns, cons_proof)
        message_splitter = self._make_splitter_for_catchup_rep(ledger, req.catchupTill)
        self._provider.send_to(rep, frm, message_splitter)

    def _get_ledger_and_id(self, req: Any) -> Tuple[int, Optional[Ledger]]:
        ledger_id = req.ledgerId
        return ledger_id, self._provider.ledger(ledger_id)

    @staticmethod
    def _make_consistency_proof(ledger: Ledger, seq_no_start: int, seq_no_end: int):
        proof = ledger.tree.consistency_proof(seq_no_start, seq_no_end)
        string_proof = [Ledger.hashToStr(p) for p in proof]
        return string_proof

    def _build_consistency_proof(self, ledger_id: int,
                                 seq_no_start: int, seq_no_end: int) -> Optional[ConsistencyProof]:
        ledger = self._provider.ledger(ledger_id)

        if seq_no_end < seq_no_start:
            logger.error("{} cannot build consistency proof: end {} is less than start {}".
                         format(self, seq_no_end, seq_no_start))
            return

        if seq_no_start > ledger.size:
            logger.error("{} cannot build consistency proof: start {} is more than ledger size {}".
                         format(self, seq_no_start, ledger.size))
            return

        if seq_no_end > ledger.size:
            logger.error("{} cannot build consistency proof: end {} is more than ledger size {}".
                         format(self, seq_no_end, ledger.size))
            return

        if seq_no_start == 0:
            # Consistency proof for an empty tree cannot exist. Using the root
            # hash now so that the node which is behind can verify that
            # TODO: Make this an empty list
            old_root = ledger.tree.root_hash
            old_root = Ledger.hashToStr(old_root)
            proof = [old_root, ]
        else:
            proof = self._make_consistency_proof(ledger, seq_no_start, seq_no_end)
            old_root = ledger.tree.merkle_tree_hash(0, seq_no_start)
            old_root = Ledger.hashToStr(old_root)

        new_root = ledger.tree.merkle_tree_hash(0, seq_no_end)
        new_root = Ledger.hashToStr(new_root)

        view_no, pp_seq_no = (0, 0)

        return ConsistencyProof(ledger_id,
                                seq_no_start,
                                seq_no_end,
                                view_no,
                                pp_seq_no,
                                old_root,
                                new_root,
                                proof)

    def _make_splitter_for_catchup_rep(self, ledger, initial_seq_no):

        def _split(message):
            txns = list(message.txns.items())
            if len(message.txns) < 2:
                logger.warning("CatchupRep has {} txn(s). This is not enough "
                               "to split. Message: {}".format(len(message.txns), message))
                return None
            divider = len(message.txns) // 2
            left = txns[:divider]
            left_last_seq_no = left[-1][0]
            right = txns[divider:]
            right_last_seq_no = right[-1][0]
            left_cons_proof = self._make_consistency_proof(ledger,
                                                           left_last_seq_no,
                                                           initial_seq_no)
            right_cons_proof = self._make_consistency_proof(ledger,
                                                            right_last_seq_no,
                                                            initial_seq_no)
            ledger_id = message.ledgerId

            left_rep = CatchupRep(ledger_id, SortedDict(left), left_cons_proof)
            right_rep = CatchupRep(ledger_id, SortedDict(right), right_cons_proof)
            return left_rep, right_rep

        return _split

    @abstractmethod
    def _on_ledger_status_up_to_date(self, ledger_id: int, frm: str):
        pass


class ClientSeederService(SeederService):
    def __init__(self, input: RxChannel, provider: CatchupDataProvider):
        SeederService.__init__(self, input, provider)

    def _on_ledger_status_up_to_date(self, ledger_id: int, frm: str):
        ledger_status = build_ledger_status(ledger_id, self._provider)
        self._provider.send_to(ledger_status, frm)


class NodeSeederService(SeederService):
    def __init__(self, input: RxChannel, provider: CatchupDataProvider):
        SeederService.__init__(self, input, provider)

    def _on_ledger_status_up_to_date(self, ledger_id: int, frm: str):
        pass
