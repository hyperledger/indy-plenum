from binascii import unhexlify

from plenum.common.types import f
from plenum.common.request import Request
from typing import Tuple, List
from plenum.common.ledger import Ledger
from plenum.common.state import State
from typing import Optional


class ReqHandler:

    def __init__(self, ledger: Ledger, state: Optional[State] = None):
        self.ledger = ledger
        self.state = state

    def commit(self, txnCount, stateRoot, txnRoot) -> List:
        """
        Commit a number of transactions
        
        :param txnCount: 
        :param stateRoot: expected state root
        :param txnRoot: 
        :return: list of committed transactions
        """

        (seqNoStart, seqNoEnd), committedTxns = self.ledger.commitTxns(txnCount)
        stateRoot = unhexlify(stateRoot.encode())
        txnRoot = self.ledger.hashToStr(unhexlify(txnRoot.encode()))
        assert self.ledger.root_hash == txnRoot
        self.state.commit(rootHash=stateRoot)
        seqNos = range(seqNoStart, seqNoEnd + 1)
        for txn, seqNo in zip(committedTxns, seqNos):
            txn[f.SEQ_NO.nm] = seqNo
        return committedTxns

    def validateReq(self, req: Request, config):
        pass

    def applyReq(self, req: Request):
        pass

    def updateState(self, txns, isCommitted=False):
        """
        Updates current state with a number of committed or 
        not committed transactions
        """
        pass
