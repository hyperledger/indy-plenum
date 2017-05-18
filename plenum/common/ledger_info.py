from collections import deque

from plenum.common.constants import LedgerState
from plenum.common.ledger import Ledger


class LedgerInfo:
    def __init__(self,
                 ledger: Ledger,
                 state: LedgerState,
                 canSync,
                 preCatchupStartClbk,
                 postCatchupStartClbk,
                 preCatchupCompleteClbk,
                 postCatchupCompleteClbk,
                 postTxnAddedToLedgerClbk,
                 verifier):

        self.ledger = ledger

        self.state = state
        self.canSync = canSync
        self.preCatchupStartClbk = preCatchupStartClbk
        self.postCatchupStartClbk = postCatchupStartClbk
        self.preCatchupCompleteClbk = preCatchupCompleteClbk
        self.postCatchupCompleteClbk = postCatchupCompleteClbk
        self.postTxnAddedToLedgerClbk = postTxnAddedToLedgerClbk
        self.verifier = verifier

        # Ledger statuses received while the ledger was not ready to be synced
        # (`canSync` was set to False)
        self.stashedLedgerStatuses = deque()

        # Tracks which nodes claim that this node's ledger status is ok
        # If a quorum of nodes (2f+1) say its up to date then mark the catchup
        #  process as completed
        self.ledgerStatusOk = set()

        # Dictionary of consistency proofs received for the ledger
        # in process of catching up
        # Key is the node name and value is a consistency proof
        self.recvdConsistencyProofs = {}

        self.catchUpTill = None

        # Catchup replies that need to be applied to the ledger
        self.receivedCatchUpReplies = []

        # Keep track of received replies from different senders
        self.recvdCatchupRepliesFrm = {}

        # Tracks the beginning of consistency proof timer. Timer starts when the
        #  node gets f+1 consistency proofs. If the node is not able to begin
        # the catchup process even after the timer expires then it requests
        # consistency proofs.
        self.consistencyProofsTimer = None

        # Tracks the beginning of catchup reply timer. Timer starts after the
        #  node sends catchup requests. If the node is not able to finish the
        # the catchup process even after the timer expires then it requests
        # missing transactions.
        self.catchupReplyTimer = None
