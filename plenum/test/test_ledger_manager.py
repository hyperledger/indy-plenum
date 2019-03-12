from plenum.common.ledger_manager import LedgerManager
from plenum.test.testable import spyable

ledger_manager_spyables = [LedgerManager.processConsistencyProof,
                           LedgerManager.processCatchupRep,
                           LedgerManager.start_catchup,
                           LedgerManager._on_ledger_sync_start,
                           LedgerManager._on_ledger_sync_complete,
                           LedgerManager._on_catchup_complete]


@spyable(methods=ledger_manager_spyables)
class TestLedgerManager(LedgerManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
