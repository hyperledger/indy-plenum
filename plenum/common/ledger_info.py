from plenum.common.ledger import Ledger


class LedgerInfo:
    def __init__(self,
                 id: int,
                 ledger: Ledger,
                 preCatchupStartClbk,
                 postCatchupCompleteClbk,
                 postTxnAddedToLedgerClbk,
                 verifier,
                 taa_acceptance_required=True):

        self.id = id
        self.ledger = ledger

        self.preCatchupStartClbk = preCatchupStartClbk
        self.postCatchupCompleteClbk = postCatchupCompleteClbk
        self.postTxnAddedToLedgerClbk = postTxnAddedToLedgerClbk
        self.verifier = verifier

        self._taa_acceptance_required = taa_acceptance_required

    @property
    def ledger_summary(self):
        return self.id, len(self.ledger), self.ledger.root_hash

    @property
    def taa_acceptance_required(self):
        return self._taa_acceptance_required
