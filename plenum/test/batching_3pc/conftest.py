import pytest

from plenum.test.conftest import getValueFromModule
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected


@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldSize = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = getValueFromModule(request, "Max3PCBatchSize", 10)

    def reset():
        tconf.Max3PCBatchSize = oldSize

    request.addfinalizer(reset)
    return tconf


@pytest.fixture(scope="module")
def client(tconf, looper, txnPoolNodeSet, client1,
           client1Connected):
    return client1Connected
