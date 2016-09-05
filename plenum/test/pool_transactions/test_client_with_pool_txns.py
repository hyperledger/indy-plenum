import pytest
from plenum.common.looper import Looper
from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame


@pytest.yield_fixture(scope="module")
def looper():
    with Looper() as l:
        yield l


def testClientUsingPoolTxns(looper, txnPoolNodeSet, poolTxnClient):
    """
    Client should not be using node registry but pool transaction file
    :return:
    """
    looper.add(poolTxnClient)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, poolTxnClient,
                                                  *txnPoolNodeSet)

