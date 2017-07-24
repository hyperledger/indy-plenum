import time

from ledger.test.conftest import tempdir
from ledger.test.test_ledger import ledger, random_txn
from plenum.common.ledger_info import LedgerInfo
from plenum.common.ledger_manager import LedgerManager
from plenum.common.messages.node_messages import ConsistencyProof


def test_missing_txn_request(ledger):
    """
    Testing LedgerManager's `_missing_txns`
    """
    for i in range(20):
        txn = random_txn(i)
        ledger.add(txn)

    # Callbacks don't matter in this test
    ledger_info = LedgerInfo(0, ledger, *[None]*6)
    assert ledger_info.catchupReplyTimer is None
    assert LedgerManager._missing_txns(ledger_info) == (False, 0)

    ledger_info.catchupReplyTimer = time.perf_counter()

    # Ledger is already ahead
    cp = ConsistencyProof(0, 1, 10, 1, 1,
                          'GJybBTHjzMzPWsE6n9qNQWAmhJP88dTcdbgkGLhYGFYn',
                          'Gv9AdSeib9EnBakfpgkU79dPMtjcnFWXvXeiCX4QAgAC', [])
    ledger_info.catchUpTill = cp
    ledger_info.receivedCatchUpReplies = [(i, {}) for i in range(1, 15)]
    assert not LedgerManager._missing_txns(ledger_info)[0]

    # Ledger is behind but catchup replies present
    cp = ConsistencyProof(0, 1, 30, 1, 1,
                          'Gv9AdSeib9EnBakfpgkU79dPMtjcnFWXvXeiCX4QAgAC',
                          'EEUnqHf2GWEpvmibiXDCZbNDSpuRgqdvCpJjgp3KFbNC', [])
    ledger_info.catchUpTill = cp
    ledger_info.receivedCatchUpReplies = [(i, {}) for i in range(21, 31)]
    assert not LedgerManager._missing_txns(ledger_info)[0]
    ledger_info.receivedCatchUpReplies = [(i, {}) for i in range(21, 35)]
    assert not LedgerManager._missing_txns(ledger_info)[0]

    # Ledger is behind
    cp = ConsistencyProof(0, 1, 30, 1, 1,
                          'Gv9AdSeib9EnBakfpgkU79dPMtjcnFWXvXeiCX4QAgAC',
                          'EEUnqHf2GWEpvmibiXDCZbNDSpuRgqdvCpJjgp3KFbNC', [])
    ledger_info.catchUpTill = cp
    ledger_info.receivedCatchUpReplies = [(i, {}) for i in range(21, 26)]
    assert LedgerManager._missing_txns(ledger_info) == (True, 5)

    ledger_info.receivedCatchUpReplies = [(i, {}) for i in range(26, 31)]
    assert LedgerManager._missing_txns(ledger_info) == (True, 5)
