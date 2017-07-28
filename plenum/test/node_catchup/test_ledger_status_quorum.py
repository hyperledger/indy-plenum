from plenum.common.ledger_manager import LedgerManager
from plenum.common.util import getMaxFailures


def test_ledger_status_quorum():
    N = 10
    f = getMaxFailures(N)
    assert not LedgerManager.has_ledger_status_quorum(f + 1, N)
    assert LedgerManager.has_ledger_status_quorum(N - f - 1, N)
