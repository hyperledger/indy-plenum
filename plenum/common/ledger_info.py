from plenum.common.ledger import Ledger


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

    @property
    def ledger_summary(self):
        return self.id, len(self.ledger), self.ledger.root_hash
