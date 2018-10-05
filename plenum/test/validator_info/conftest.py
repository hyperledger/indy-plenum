import pytest

from plenum.common.constants import TXN_TYPE, DATA, \
    CURRENT_PROTOCOL_VERSION, DOMAIN_LEDGER_ID
from plenum.common.request import Request
from plenum.common.types import f
from plenum.common.util import getTimeBasedId
from plenum.test import waits
from plenum.test.helper import check_sufficient_replies_received
from plenum.test.node_catchup.helper import ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.test_client import genTestClient
from stp_core.loop.eventually import eventually


TEST_NODE_NAME = 'Alpha'
INFO_FILENAME = '{}_info.json'.format(TEST_NODE_NAME.lower())
PERIOD_SEC = 1

@pytest.fixture(scope='function')
def info(node):
    return node._info_tool.info


@pytest.fixture(scope='module')
def node(txnPoolNodeSet):
    for n in txnPoolNodeSet:
        if n.name == TEST_NODE_NAME:
            return n
    assert False, 'Pool does not have "{}" node'.format(TEST_NODE_NAME)


@pytest.fixture
def read_txn_and_get_latest_info(txnPoolNodesLooper,
                                 client_and_wallet, node):
    client, wallet = client_and_wallet

    def read_wrapped(txn_type):
        op = {
            TXN_TYPE: txn_type,
            f.LEDGER_ID.nm: DOMAIN_LEDGER_ID,
            DATA: 1
        }
        req = Request(identifier=wallet.defaultId,
                      operation=op, reqId=getTimeBasedId(),
                      protocolVersion=CURRENT_PROTOCOL_VERSION)
        client.submitReqs(req)

        timeout = waits.expectedTransactionExecutionTime(
            len(client.inBox))

        txnPoolNodesLooper.run(
            eventually(check_sufficient_replies_received,
                       client, req.identifier, req.reqId,
                       retryWait=1, timeout=timeout))
        return node._info_tool.info

    return read_wrapped


@pytest.fixture(scope="function")
def load_latest_info(node):
    def wrapped():
        return node._info_tool.info

    return wrapped


@pytest.fixture
def client_and_wallet(txnPoolNodesLooper, tdirWithClientPoolTxns, txnPoolNodeSet):
    client, wallet = genTestClient(tmpdir=tdirWithClientPoolTxns, nodes=txnPoolNodeSet,
                                   name='reader', usePoolLedger=True)
    txnPoolNodesLooper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(txnPoolNodesLooper, client,
                                                  *txnPoolNodeSet)
    return client, wallet
