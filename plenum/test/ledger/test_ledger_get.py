from copy import deepcopy

import pytest

from plenum.common.constants import CONFIG_LEDGER_ID
from plenum.test.ledger.conftest import create_txns


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
