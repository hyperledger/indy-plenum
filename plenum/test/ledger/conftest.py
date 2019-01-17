import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.txn_util import reqToTxn
from plenum.test.helper import sdk_signed_random_requests

NUM_BATCHES = 3
TXNS_IN_BATCH = 5


def create_txns(looper, sdk_wallet_client):
    reqs = sdk_signed_random_requests(looper, sdk_wallet_client, TXNS_IN_BATCH)
    return [reqToTxn(req) for req in reqs]


@pytest.fixture(scope='module')
def ledger(txnPoolNodeSet):
    return txnPoolNodeSet[0].ledgers[DOMAIN_LEDGER_ID]


@pytest.fixture(scope='module')
def ledger_with_batches_appended(ledger,
                                 looper, sdk_wallet_client):
    for i in range(NUM_BATCHES):
        txns = create_txns(looper, sdk_wallet_client)
        for txn in txns:
            ledger.append_metadata(txn)
            ledger.append_txn(txn)
    return ledger
