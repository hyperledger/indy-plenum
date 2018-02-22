import pytest

from plenum.common.util import randomString
from plenum.test.test_node import checkNodesConnected
from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import addNewStewardAndNode, \
    buildPoolClientAndWallet, addNewSteward, sdk_add_new_steward_and_node, sdk_pool_refresh


@pytest.fixture(scope="module")
def tconf(tconf, request):
    # Lowering DELTA since some requests will result in validation errors and
    # that will decrease master throughput.
    # TODO: When monitoring metrics are calibrated, these things
    # should be taken care of.
    tconf.DELTA = .6
    return tconf


@pytest.yield_fixture(scope="module")
def looper(txnPoolNodesLooper):
    yield txnPoolNodesLooper


@pytest.fixture(scope="module")
def stewardAndWallet1(looper, txnPoolNodeSet, poolTxnStewardData,
                      tdirWithClientPoolTxns, client_tdir):
    client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                              client_tdir)
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
def nodeThetaAdded(looper, txnPoolNodeSet, tdir, client_tdir,
                   tconf, steward1, stewardWallet, allPluginsPath, testNodeClass=None,
                   testClientClass=None, name=None):
    newStewardName = "testClientSteward" + randomString(3)
    newNodeName = name or "Theta"
    newSteward, newStewardWallet, newNode = addNewStewardAndNode(looper,
                                                                 steward1,
                                                                 stewardWallet,
                                                                 newStewardName,
                                                                 newNodeName,
                                                                 tdir,
                                                                 client_tdir,
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


@pytest.fixture(scope='module')
def sdk_node_theta_added(looper,
                         txnPoolNodeSet,
                         tdir,
                         tconf,
                         sdk_pool_handle,
                         sdk_wallet_steward,
                         allPluginsPath,
                         testNodeClass=None,
                         name=None):
    new_steward_name = "testClientSteward" + randomString(3)
    new_node_name = name or "Theta"
    new_steward_wallet, new_node = \
        sdk_add_new_steward_and_node(looper,
                                     sdk_pool_handle,
                                     sdk_wallet_steward,
                                     new_steward_name,
                                     new_node_name,
                                     tdir,
                                     tconf,
                                     allPluginsPath,
                                     nodeClass=testNodeClass)
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    sdk_pool_refresh(looper, sdk_pool_handle)
    return new_steward_wallet, new_node


@pytest.fixture(scope="module")
def clientAndWallet1(txnPoolNodeSet, poolTxnClientData, tdirWithClientPoolTxns, client_tdir):
    client, wallet = buildPoolClientAndWallet(poolTxnClientData,
                                              client_tdir)
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
def newAdHocSteward(looper, client_tdir, steward1, stewardWallet):
    newStewardName = "testClientSteward" + randomString(3)
    newSteward, newStewardWallet = addNewSteward(looper, client_tdir, steward1,
                                                 stewardWallet,
                                                 newStewardName)
    return newSteward, newStewardWallet
