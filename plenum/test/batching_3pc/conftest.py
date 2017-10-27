import pytest

from plenum.test.conftest import getValueFromModule
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected


@pytest.fixture(scope="module")
def tconf(conf, request):
    oldSize = conf.Max3PCBatchSize
    conf.Max3PCBatchSize = getValueFromModule(request, "Max3PCBatchSize", 10)

    def reset():
        conf.Max3PCBatchSize = oldSize

    request.addfinalizer(reset)
    return conf


@pytest.fixture(scope="module")
def client(conf, looper, txnPoolNodeSet, client1,
           client1Connected):
    return client1Connected
