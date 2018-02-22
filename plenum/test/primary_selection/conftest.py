import pytest

from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.primary_selection.helper import check_newly_added_nodes, \
    getPrimaryNodesIdxs
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper, nodeThetaAdded, \
    stewardAndWallet1, steward1, stewardWallet
from plenum.test.pool_transactions.helper import buildPoolClientAndWallet


@pytest.fixture(scope="module")
def one_node_added(looper, txnPoolNodeSet, nodeThetaAdded):
    # New node knows primary same primary as others and has rank greater
    # than others
    _, _, new_node = nodeThetaAdded
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
    check_newly_added_nodes(looper, txnPoolNodeSet, [new_node])
    return new_node


@pytest.fixture(scope='module')
def sdk_one_node_added(looper, txnPoolNodeSet, sdk_node_theta_added):
    # New node knows primary same primary as others and has rank greater
    # than others
    _, new_node = sdk_node_theta_added
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
    check_newly_added_nodes(looper, txnPoolNodeSet, [new_node])
    return new_node


@pytest.fixture(scope="module")
def txnPoolMasterNodes(txnPoolNodeSet):
    primariesIdxs = getPrimaryNodesIdxs(txnPoolNodeSet)
    return txnPoolNodeSet[primariesIdxs[0]], txnPoolNodeSet[primariesIdxs[1]]


@pytest.fixture(scope="module")
def stewardAndWalletForMasterNode(looper, poolTxnData, poolTxnStewardNames,
                                  tdirWithClientPoolTxns, txnPoolNodeSet, txnPoolMasterNodes):
    primariesIdxs = getPrimaryNodesIdxs(txnPoolNodeSet)
    master_node = txnPoolMasterNodes[0]
    stewardName = poolTxnStewardNames[primariesIdxs[0]]
    stewardsSeed = poolTxnData["seeds"][stewardName].encode()

    stewardClient, stewardWallet = buildPoolClientAndWallet(
        (stewardName, stewardsSeed), tdirWithClientPoolTxns)
    looper.add(stewardClient)
    looper.run(stewardClient.ensureConnectedToNodes())

    return stewardClient, stewardWallet
