from plenum.test.ledger.conftest import TXNS_IN_BATCH, create_txns


def test_append_empty(ledger):
    size = ledger.seqNo
    (start, end), appended_txns = ledger.appendTxns([])
    assert start == size
    assert end == size
    assert appended_txns == []


def test_append_result(ledger,
                       looper, sdk_wallet_client):
    size = ledger.seqNo
    txns1 = create_txns(looper, sdk_wallet_client)
    ledger.append_txns_metadata(txns1)
    (start, end), appended_txns = ledger.appendTxns(txns1)
    assert start == size + 1
    assert end == size + TXNS_IN_BATCH
    assert len(appended_txns) == TXNS_IN_BATCH

    txns2 = create_txns(looper, sdk_wallet_client)
    ledger.append_txns_metadata(txns2)
    (start, end), appended_txns = ledger.appendTxns(txns2)
    assert start == size + 1 + TXNS_IN_BATCH
    assert end == size + TXNS_IN_BATCH + TXNS_IN_BATCH
    assert len(appended_txns) == TXNS_IN_BATCH
