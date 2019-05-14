from plenum.common.ledger_manager import LedgerManager
from plenum.test.testable import spyable
from plenum.test.testing_utils import FakeSomething

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


def test_taa_acceptance_required_passed_to_ledger_info():
    fakeNode = FakeSomething(timer=None, allNodeNames=set('Node1'))
    lm = LedgerManager(fakeNode)
    setattr(fakeNode, 'ledgerManager', lm)

    lm.addLedger(0, FakeSomething(hasher=None))
    assert lm.ledgerRegistry[0].taa_acceptance_required == True

    lm.addLedger(1, FakeSomething(hasher=None), taa_acceptance_required=False)
    assert lm.ledgerRegistry[1].taa_acceptance_required == False

    lm.addLedger(2, FakeSomething(hasher=None), taa_acceptance_required=True)
    assert lm.ledgerRegistry[2].taa_acceptance_required == True
