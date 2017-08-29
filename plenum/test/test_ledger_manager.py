from plenum.common.ledger_manager import LedgerManager
from plenum.test.testable import spyable

ledger_manager_spyables = [LedgerManager.startCatchUpProcess,
                           LedgerManager.catchupCompleted,
                           LedgerManager.processConsistencyProof,
                           LedgerManager.canProcessConsistencyProof,
                           LedgerManager.processCatchupRep,
                           LedgerManager.getCatchupReqs
                           ]


@spyable(methods=ledger_manager_spyables)
class TestLedgerManager(LedgerManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
