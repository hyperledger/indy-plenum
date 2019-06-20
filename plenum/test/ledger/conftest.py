import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.txn_util import reqToTxn
from plenum.test.helper import sdk_signed_random_requests

NUM_BATCHES = 3
TXNS_IN_BATCH = 5


def create_txns(looper, sdk_wallet_client, count=TXNS_IN_BATCH):
    reqs = sdk_signed_random_requests(looper, sdk_wallet_client, count)
    return [reqToTxn(req) for req in reqs]


@pytest.fixture(scope='module')
def created_txns(ledger, looper, sdk_wallet_client):
    txns = []
    for i in range(NUM_BATCHES):
        txns.append(create_txns(looper, sdk_wallet_client, TXNS_IN_BATCH))
    return txns


@pytest.fixture(scope='module')
def ledger(txnPoolNodeSet):
    return txnPoolNodeSet[0].getLedger(DOMAIN_LEDGER_ID)


@pytest.fixture(scope='module')
def inital_size(ledger):
    return ledger.size


@pytest.fixture(scope='module')
def inital_root_hash(ledger):
    return ledger.tree.root_hash


@pytest.fixture(scope='module')
def ledger_with_batches_appended(ledger,
                                 created_txns):
    for txn_batch in created_txns:
        ledger.append_txns_metadata(txn_batch)
        ledger.appendTxns(txn_batch)
    return ledger
