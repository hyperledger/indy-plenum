import pytest

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.util import randomString
from plenum.test.conftest import getValueFromModule
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import \
    addNewStewardAndNode, buildPoolClientAndWallet
from plenum.test.pool_transactions.conftest import stewardAndWallet1, \
    steward1, stewardWallet
from plenum.test.test_client import TestClient
from plenum.test.test_node import checkNodesConnected


def whitelist():
    return ['got error while verifying message']


logger = getlogger()


@pytest.yield_fixture(scope="module")
def looper(txnPoolNodesLooper):
    yield txnPoolNodesLooper


@pytest.yield_fixture("module")
def nodeCreatedAfterSomeTxns(looper, txnPoolNodesLooper, txnPoolNodeSet,
                             tdirWithPoolTxns, poolTxnStewardData, tconf,
                             allPluginsPath, request):
    client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                              tdirWithPoolTxns,
                                              clientClass=TestClient)
    looper.add(client)
    looper.run(client.ensureConnectedToNodes())
    txnCount = getValueFromModule(request, "txnCount", 5)
    sendReqsToNodesAndVerifySuffReplies(txnPoolNodesLooper,
                                        wallet,
                                        client,
                                        txnCount)
    newStewardName = randomString()
    newNodeName = "Epsilon"
    newStewardClient, newStewardWallet, newNode = addNewStewardAndNode(
        looper, client, wallet, newStewardName, newNodeName,
        tdirWithPoolTxns, tconf, allPluginsPath=allPluginsPath, autoStart=True)
    yield looper, newNode, client, wallet, newStewardClient, \
        newStewardWallet


@pytest.fixture("module")
def nodeSetWithNodeAddedAfterSomeTxns(txnPoolNodeSet, nodeCreatedAfterSomeTxns):
    looper, newNode, client, wallet, newStewardClient, newStewardWallet = \
        nodeCreatedAfterSomeTxns
    txnPoolNodeSet.append(newNode)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    looper.run(newStewardClient.ensureConnectedToNodes())
    looper.run(client.ensureConnectedToNodes())
    return looper, newNode, client, wallet, newStewardClient, newStewardWallet


@pytest.fixture("module")
def newNodeCaughtUp(txnPoolNodeSet, nodeSetWithNodeAddedAfterSomeTxns):
    looper, newNode, _, _, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:4])
    return newNode
