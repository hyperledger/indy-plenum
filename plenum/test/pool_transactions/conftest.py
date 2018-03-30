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
                                     allPluginsPath)
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    sdk_pool_refresh(looper, sdk_pool_handle)
    return new_steward_wallet, new_node


@pytest.fixture(scope="function")
def newAdHocSteward(looper, client_tdir, steward1, stewardWallet):
    newStewardName = "testClientSteward" + randomString(3)
    newSteward, newStewardWallet = addNewSteward(looper, client_tdir, steward1,
                                                 stewardWallet,
                                                 newStewardName)
    return newSteward, newStewardWallet
