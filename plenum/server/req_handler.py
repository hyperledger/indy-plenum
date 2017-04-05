from binascii import unhexlify

from plenum.common.types import f
from plenum.common.request import Request
from typing import List
from plenum.common.ledger import Ledger
from plenum.common.state import PruningState


class RequestHandler:
    """
    Base class for request handlers
    Declares methods for validation, application of requests and 
    state control
    """

    def __init__(self, ledger: Ledger, state: PruningState):
        # TODO: continue using PruningState until State hierarchy fixed
        self.ledger = ledger
        self.state = state

    def validate(self, req: Request, config):
        pass

    def applyReq(self, req: Request):
        pass

    def updateState(self, txns, isCommitted=False):
        """
        Updates current state with a number of committed or 
        not committed transactions
        """
        pass

    def commit(self, txnCount, stateRoot, txnRoot) -> List:
        """
        Commit a number of transactions

        :param txnCount: 
        :param stateRoot: expected state root
        :param txnRoot: 
        :return: list of committed transactions
        """

        (seqNoStart, seqNoEnd), committedTxns = \
            self.ledger.commitTxns(txnCount)
        stateRoot = unhexlify(stateRoot.encode())
        txnRoot = self.ledger.hashToStr(unhexlify(txnRoot.encode()))
        assert self.ledger.root_hash == txnRoot
        self.state.commit(rootHash=stateRoot)
        seqNos = range(seqNoStart, seqNoEnd + 1)
        for txn, seqNo in zip(committedTxns, seqNos):
            txn[f.SEQ_NO.nm] = seqNo
        return committedTxns