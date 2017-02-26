import pytest

from plenum.common.eventually import eventually
from plenum.common.util import randomString
from plenum.test.conftest import getValueFromModule
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality
from plenum.test.pool_transactions.helper import \
    addNewStewardAndNode, buildPoolClientAndWallet
from plenum.test.test_client import TestClient
from plenum.test.test_node import checkNodesConnected


def whitelist():
    return ['got error while verifying message']


@pytest.yield_fixture("module")
def nodeCreatedAfterSomeTxns(txnPoolNodesLooper, txnPoolNodeSet,
                             tdirWithPoolTxns, poolTxnStewardData, tconf,
                             allPluginsPath, request):
    # with Looper(debug=True) as looper:
    client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                              tdirWithPoolTxns,
                                              clientClass=TestClient)
    txnPoolNodesLooper.add(client)
    txnPoolNodesLooper.run(client.ensureConnectedToNodes())
    txnCount = getValueFromModule(request, "txnCount", 5)
    sendReqsToNodesAndVerifySuffReplies(txnPoolNodesLooper, wallet, client,
                                        txnCount, timeoutPerReq=25)

    newStewardName = randomString()
    newNodeName = "Epsilon"
    newStewardClient, newStewardWallet, newNode = addNewStewardAndNode(
        txnPoolNodesLooper, client, wallet, newStewardName, newNodeName,
        tdirWithPoolTxns, tconf, allPluginsPath=allPluginsPath, autoStart=True)
    yield txnPoolNodesLooper, newNode, client, wallet, newStewardClient, \
          newStewardWallet


@pytest.fixture("module")
def nodeSetWithNodeAddedAfterSomeTxns(txnPoolNodeSet, nodeCreatedAfterSomeTxns):
    looper, newNode, client, wallet, newStewardClient, newStewardWallet = \
        nodeCreatedAfterSomeTxns
    txnPoolNodeSet.append(newNode)
    looper.run(checkNodesConnected(txnPoolNodeSet, overrideTimeout=10))
    looper.run(newStewardClient.ensureConnectedToNodes())
    looper.run(client.ensureConnectedToNodes())
    return looper, newNode, client, wallet, newStewardClient, newStewardWallet


@pytest.fixture("module")
def newNodeCaughtUp(txnPoolNodeSet, nodeSetWithNodeAddedAfterSomeTxns):
    looper, newNode, _, _, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    looper.run(eventually(checkNodeLedgersForEquality, newNode,
                          *txnPoolNodeSet[:4], retryWait=1, timeout=10))
    return newNode
