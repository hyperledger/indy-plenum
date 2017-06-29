from plenum.common.ledger_manager import LedgerManager
from plenum.test.testable import spyable


@spyable(methods=[LedgerManager.startCatchUpProcess,
                  LedgerManager.catchupCompleted,
                  LedgerManager.processConsistencyProof,
                  LedgerManager.processConsistencyProofReq,
                  LedgerManager.canProcessConsistencyProof,
                  LedgerManager.processCatchupRep,
                  LedgerManager.getCatchupReqs
                  ])
class TestLedgerManager(LedgerManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)