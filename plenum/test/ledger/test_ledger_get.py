from copy import deepcopy

import pytest

from plenum.common.constants import CONFIG_LEDGER_ID
from plenum.test.ledger.conftest import create_txns, TXNS_IN_BATCH, NUM_BATCHES


@pytest.fixture(scope='module')
def ledger(txnPoolNodeSet):
    return txnPoolNodeSet[0].ledgers[CONFIG_LEDGER_ID]


def test_get_last_txn(ledger,
                      looper, sdk_wallet_client):
    # add 2 txns uncommitted
    txn1 = create_txns(looper, sdk_wallet_client, 1)[0]
    ledger.append_txns_metadata([txn1])
    expected_txn1 = deepcopy(txn1)
    ledger.appendTxns([txn1])

    txn2 = create_txns(looper, sdk_wallet_client, 1)[0]
    ledger.append_txns_metadata([txn2])
    expected_last_added_txn = deepcopy(txn2)
    expected_txn2 = deepcopy(txn2)
    ledger.appendTxns([txn2])

    # get last txn (expected - txn2)
    assert expected_last_added_txn == ledger.get_last_txn()
    assert ledger.get_last_committed_txn() is None

    # commit 1st txn
    ledger.commitTxns(1)

    # get last txn (expected - txn2)
    assert expected_last_added_txn == ledger.get_last_txn()
    a = ledger.get_last_committed_txn()
    assert expected_txn1 == ledger.get_last_committed_txn()

    # commit 2d txn
    ledger.commitTxns(1)

    # get last txn (expected - txn2)
    assert expected_last_added_txn == ledger.get_last_txn()
    assert expected_txn2 == ledger.get_last_committed_txn()


def get_unexistent_txn(ledger_with_batches_appended, inital_size):
    ledger = ledger_with_batches_appended
    with pytest.raises(KeyError):
        ledger.get_by_seq_no_uncommitted(inital_size + NUM_BATCHES * TXNS_IN_BATCH + 1)
    with pytest.raises(KeyError):
        ledger.get_by_seq_no_uncommitted(0)
    with pytest.raises(KeyError):
        ledger.get_by_seq_no_uncommitted(-1)


def get_committed_txns(ledger_with_batches_appended, inital_size):
    ledger = ledger_with_batches_appended
    assert ledger.get_by_seq_no_uncommitted(1)
    assert ledger.get_by_seq_no_uncommitted(inital_size)


def get_uncommitted_txns(ledger_with_batches_appended, inital_size, created_txns):
    ledger = ledger_with_batches_appended
    assert ledger.get_by_seq_no_uncommitted(inital_size + 1) == created_txns[0][1]
    assert ledger.get_by_seq_no_uncommitted(inital_size + 2) == created_txns[0][2]
    assert ledger.get_by_seq_no_uncommitted(inital_size + TXNS_IN_BATCH + 3) == created_txns[1][3]
    assert ledger.get_by_seq_no_uncommitted(inital_size + (NUM_BATCHES - 1) * TXNS_IN_BATCH) == \
           created_txns[NUM_BATCHES - 1][0]
    assert ledger.get_by_seq_no_uncommitted(inital_size + (NUM_BATCHES - 1) * TXNS_IN_BATCH + 1) == \
           created_txns[NUM_BATCHES - 1][1]
    assert ledger.get_by_seq_no_uncommitted(inital_size + NUM_BATCHES * TXNS_IN_BATCH) == \
           created_txns[NUM_BATCHES][TXNS_IN_BATCH]
