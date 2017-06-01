import pytest
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected


@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldSize = tconf.Max3PCBatchSize
    oldTIme = tconf.Max3PCBatchWait
    tconf.Max3PCBatchSize = 3
    tconf.Max3PCBatchWait = 5

    def reset():
        tconf.Max3PCBatchSize = oldSize
        tconf.Max3PCBatchWait = oldTIme

    request.addfinalizer(reset)
    return tconf


@pytest.fixture(scope="module")
def client(tconf, looper, txnPoolNodeSet, client1,
                                client1Connected):
    return client1Connected
