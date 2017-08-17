from typing import List

import base58

from plenum.common.ledger import Ledger
from plenum.common.request import Request
from plenum.persistence.util import txnsWithSeqNo
from stp_core.common.log import getlogger

from state.state import State

logger = getlogger()


class RequestHandler:
    """
    Base class for request handlers
    Declares methods for validation, application of requests and
    state control
    """

    def __init__(self, ledger: Ledger, state: State):
        self.ledger = ledger
        self.state = state

    def validate(self, req: Request, config=None):
        """
        Validates request. Raises exception if request is invalid.
        """

    def apply(self, req: Request, cons_time: int):
        """
        Applies request
        """

    def updateState(self, txns, isCommitted=False):
        """
        Updates current state with a number of committed or
        not committed transactions
        """

    def commit(self, txnCount, stateRoot, txnRoot) -> List:
        """
        :param txnCount: The number of requests to commit (The actual requests are
        picked up from the uncommitted list from the ledger)
        :param stateRoot: The state trie root after the txns are committed
        :param txnRoot: The txn merkle root after the txns are committed

        :return: list of committed transactions
        """

        (seqNoStart, seqNoEnd), committedTxns = \
            self.ledger.commitTxns(txnCount)
        stateRoot = base58.b58decode(stateRoot.encode())
        # Probably the following assertion fail should trigger catchup
        assert self.ledger.root_hash == txnRoot, '{} {}'.format(
            self.ledger.root_hash, txnRoot)
        self.state.commit(rootHash=stateRoot)
        return txnsWithSeqNo(seqNoStart, seqNoEnd, committedTxns)

    def onBatchCreated(self, stateRoot):
        pass

    def onBatchRejected(self):
        pass
