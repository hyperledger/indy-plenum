import pytest

from plenum.common.ledger_info import LedgerInfo
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


@pytest.fixture
def ledger_manager():
    fakeNode = FakeSomething(timer=None, allNodeNames=set('Node1'))
    lm = LedgerManager(fakeNode)
    setattr(fakeNode, 'ledgerManager', lm)
    return lm


def test_ledger_info(ledger_manager):
    ledger_manager.addLedger(
        0, FakeSomething(hasher=None))
    assert isinstance(ledger_manager.ledger_info(0), LedgerInfo)
    assert ledger_manager.ledger_info(1) is None


def test_taa_acceptance_required_passed_to_ledger_info(ledger_manager):
    ledger_manager.addLedger(
        0, FakeSomething(hasher=None))
    assert ledger_manager.ledger_info(0).taa_acceptance_required == True

    ledger_manager.addLedger(
        1, FakeSomething(hasher=None), taa_acceptance_required=False)
    assert ledger_manager.ledger_info(1).taa_acceptance_required == False

    ledger_manager.addLedger(
        2, FakeSomething(hasher=None), taa_acceptance_required=True)
    assert ledger_manager.ledger_info(2).taa_acceptance_required == True
