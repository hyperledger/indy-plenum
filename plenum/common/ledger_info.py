from plenum.common.constants import LedgerState
from plenum.common.ledger import Ledger


# TODO: Choose a better name, its not just information about a ledger, its more
#  of a handle
class LedgerInfo:
    def __init__(self,
                 id: int,
                 ledger: Ledger,
                 preCatchupStartClbk,
                 postCatchupCompleteClbk,
                 postTxnAddedToLedgerClbk,
                 verifier):

        self.id = id
        self.ledger = ledger

        self.preCatchupStartClbk = preCatchupStartClbk
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

        # Number of transactions caught up
        self.num_txns_caught_up = 0

    @property
    def ledger_summary(self):
        return self.id, len(self.ledger), self.ledger.root_hash
