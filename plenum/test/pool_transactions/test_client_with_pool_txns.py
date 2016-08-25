import pytest
from plenum.common.looper import Looper


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
    looper.run(poolTxnClient.ensureConnectedToNodes())

