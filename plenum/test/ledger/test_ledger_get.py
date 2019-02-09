from copy import deepcopy

from plenum.test.ledger.conftest import create_txns


def test_get_last_txn(ledger,
                      looper, sdk_wallet_client):
    # add 2 txns uncommitted
    txn1 = create_txns(looper, sdk_wallet_client, 1)[0]
    ledger.append_txns_metadata([txn1])
    ledger.appendTxns([txn1])

    txn2 = create_txns(looper, sdk_wallet_client, 1)[0]
    ledger.append_txns_metadata([txn2])
    expected_txn = deepcopy(txn2)
    ledger.appendTxns([txn2])

    # get last txn (expected - txn2)
    assert expected_txn == ledger.get_last_txn()

    # commit 1st txn
    ledger.commitTxns(1)

    # get last txn (expected - txn2)
    assert expected_txn == ledger.get_last_txn()

    # commit 2d txn
    ledger.commitTxns(1)

    # get last txn (expected - txn2)
    assert expected_txn == ledger.get_last_txn()
