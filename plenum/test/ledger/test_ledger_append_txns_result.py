import pytest

from common.exceptions import LogicError
from plenum.test.ledger.conftest import TXNS_IN_BATCH, create_txns


def test_append_empty(ledger):
    size = ledger.seqNo
    with pytest.raises(LogicError):
        ledger.append_txn(None)
    assert size == ledger.seqNo


def test_append_result(ledger,
                       looper, sdk_wallet_client):
    size = ledger.seqNo
    txns1 = create_txns(looper, sdk_wallet_client)
    start = end = appended_txns = 0
    for i, txn in enumerate(txns1):
        ledger.append_metadata(txn)
        seq_no, txn = ledger.append_txn(txn)
        if i == 0:
            start = seq_no
        if i == len(txns1) - 1:
            end = seq_no
        appended_txns += 1
    assert start == size + 1
    assert end == size + TXNS_IN_BATCH
    assert appended_txns == TXNS_IN_BATCH

    start = end = appended_txns = 0
    txns2 = create_txns(looper, sdk_wallet_client)
    for i, txn in enumerate(txns2):
        ledger.append_metadata(txn)
        seq_no, txn = ledger.append_txn(txn)
        if i == 0:
            start = seq_no
        if i == len(txns1) - 1:
            end = seq_no
        appended_txns += 1
    assert start == size + 1 + TXNS_IN_BATCH
    assert end == size + TXNS_IN_BATCH + TXNS_IN_BATCH
    assert appended_txns == TXNS_IN_BATCH
