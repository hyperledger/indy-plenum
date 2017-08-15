import pytest

from stp_core.loop.looper import Looper
from plenum.common.util import randomString
from plenum.test.test_node import checkNodesConnected
from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import addNewStewardAndNode, \
    buildPoolClientAndWallet, addNewSteward


@pytest.fixture(scope="module")
def tconf(conf, tdir, request):
    conf.baseDir = tdir
    # Lowering DELTA since some requests will result in validation errors and
    # that will decrease master throughput.
    # TODO: When monitoring metrics are calibrated, these things
    # should be taken care of.
    conf.DELTA = .6
    return conf


@pytest.yield_fixture(scope="module")
def looper(txnPoolNodesLooper):
    yield txnPoolNodesLooper


@pytest.fixture(scope="module")
def stewardAndWallet1(looper, txnPoolNodeSet, poolTxnStewardData,
                      tdirWithPoolTxns):
    client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                              tdirWithPoolTxns)
    yield client, wallet
    client.stop()


@pytest.fixture(scope="module")
def steward1(looper, txnPoolNodeSet, stewardAndWallet1):
    steward, wallet = stewardAndWallet1
    looper.add(steward)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward,
                                                  *txnPoolNodeSet)
    return steward


@pytest.fixture(scope="module")
def stewardWallet(stewardAndWallet1):
    return stewardAndWallet1[1]


@pytest.fixture("module")
def nodeThetaAdded(looper, txnPoolNodeSet, tdirWithPoolTxns, tconf, steward1,
                   stewardWallet, allPluginsPath, testNodeClass=None,
                   testClientClass=None, name=None):
    newStewardName = "testClientSteward" + randomString(3)
    newNodeName = name or "Theta"
    newSteward, newStewardWallet, newNode = addNewStewardAndNode(looper,
                                                                 steward1,
                                                                 stewardWallet,
                                                                 newStewardName,
                                                                 newNodeName,
                                                                 tdirWithPoolTxns,
                                                                 tconf,
                                                                 allPluginsPath,
                                                                 nodeClass=testNodeClass,
                                                                 clientClass=testClientClass)
    txnPoolNodeSet.append(newNode)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)
    return newSteward, newStewardWallet, newNode


@pytest.fixture(scope="module")
def clientAndWallet1(txnPoolNodeSet, poolTxnClientData, tdirWithPoolTxns):
    client, wallet = buildPoolClientAndWallet(poolTxnClientData,
                                              tdirWithPoolTxns)
    yield client, wallet
    client.stop()


@pytest.fixture(scope="module")
def client1(clientAndWallet1):
    return clientAndWallet1[0]


@pytest.fixture(scope="module")
def wallet1(clientAndWallet1):
    return clientAndWallet1[1]


@pytest.fixture(scope="module")
def client1Connected(looper, client1):
    looper.add(client1)
    looper.run(client1.ensureConnectedToNodes())
    return client1


@pytest.fixture(scope="function")
def newAdHocSteward(looper, tdir, steward1, stewardWallet):
    newStewardName = "testClientSteward" + randomString(3)
    newSteward, newStewardWallet = addNewSteward(looper, tdir, steward1,
                                                 stewardWallet,
                                                 newStewardName)
    return newSteward, newStewardWallet
