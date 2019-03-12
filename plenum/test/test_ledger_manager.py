from plenum.common.ledger_manager import LedgerManager
from plenum.test.testable import spyable

ledger_manager_spyables = [LedgerManager.processConsistencyProof,
                           LedgerManager.processCatchupRep,
                           LedgerManager.catchup_ledger,
                           LedgerManager._on_leecher_service_stop]


@spyable(methods=ledger_manager_spyables)
class TestLedgerManager(LedgerManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
