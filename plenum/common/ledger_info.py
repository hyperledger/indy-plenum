from plenum.common.constants import LedgerState
from plenum.common.ledger import Ledger


# TODO: Choose a better name, its not just information about a ledger, its more
#  of a handle
class LedgerInfo:
    def __init__(self,
                 id: int,
                 ledger: Ledger,
                 preCatchupStartClbk,
                 postCatchupStartClbk,
                 preCatchupCompleteClbk,
                 postCatchupCompleteClbk,
                 postTxnAddedToLedgerClbk,
                 verifier):

        self.id = id
        self.ledger = ledger

        self.preCatchupStartClbk = preCatchupStartClbk
        self.postCatchupStartClbk = postCatchupStartClbk
        self.preCatchupCompleteClbk = preCatchupCompleteClbk
        self.postCatchupCompleteClbk = postCatchupCompleteClbk
        self.postTxnAddedToLedgerClbk = postTxnAddedToLedgerClbk
        self.verifier = verifier

        self.set_defaults()

    # noinspection PyAttributeOutsideInit
    def set_defaults(self):
        self.state = LedgerState.not_synced
        # Setting `canSync` to False since each ledger is synced in an
        # established order so `canSync` will be set to True accordingly.
        self.canSync = False

        # Tracks which nodes claim that this node's ledger status is ok
        # If a quorum of nodes (2f+1) say its up to date then mark the catchup
        #  process as completed
        self.ledgerStatusOk = set()

        # Key of the 3PC-batch ordered by the master instance that contained
        # the last transaction of this node's ledger
        # This is a map of last 3PC for each received LedgerStatus
        self.last_txn_3PC_key = {}

        # Dictionary of consistency proofs received for the ledger
        # in process of catching up
        # Key is the node name and value is a consistency proof
        self.recvdConsistencyProofs = {}

        # Tracks the consistency proof till which the node has to catchup
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

        # Number of transactions caught up
        self.num_txns_caught_up = 0

    def pre_syncing(self):
        if self.preCatchupStartClbk:
            self.preCatchupStartClbk()

    # noinspection PyAttributeOutsideInit
    def done_syncing(self):
        self.canSync = False
        self.state = LedgerState.synced
        self.ledgerStatusOk = set()
        self.last_txn_3PC_key = {}
        self.recvdConsistencyProofs = {}
        self.receivedCatchUpReplies = []
        self.recvdCatchupRepliesFrm = {}
        if self.postCatchupCompleteClbk:
            self.postCatchupCompleteClbk()
        self.catchupReplyTimer = None
        if self.catchUpTill:
            cp = self.catchUpTill
            self.num_txns_caught_up = cp.seqNoEnd - cp.seqNoStart
        self.catchUpTill = None

    @property
    def ledger_summary(self):
        return self.id, len(self.ledger), self.ledger.root_hash
