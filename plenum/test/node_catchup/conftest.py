import pytest
from plenum.common.looper import Looper
from plenum.common.util import randomString
from plenum.common.port_dispenser import genHa
from plenum.test.conftest import getValueFromModule
from plenum.test.eventually import eventually
from plenum.test.helper import TestClient, genHa, \
    sendReqsToNodesAndVerifySuffReplies, checkNodesConnected
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality
from plenum.test.pool_transactions.helper import \
    addNewStewardAndNode, buildPoolClientAndWallet


@pytest.yield_fixture("module")
def nodeCreatedAfterSomeTxns(txnPoolNodeSet, tdirWithPoolTxns,
                             poolTxnStewardData, tconf,
                             allPluginsPath, request):
    with Looper(debug=True) as looper:
        client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                                  tdirWithPoolTxns,
                                                  clientClass=TestClient)
        looper.add(client)
        looper.run(client.ensureConnectedToNodes())
        txnCount = getValueFromModule(request, "txnCount", 5)
        sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, txnCount,
                                            timeoutPerReq=25)

        newStewardName = randomString()
        newNodeName = "Epsilon"
        newStewardClient, newStewardWallet, newNode = addNewStewardAndNode(
            looper, client,
            wallet,
            newStewardName,
            newNodeName,
            tdirWithPoolTxns,
            tconf,
            allPluginsPath=allPluginsPath,
            autoStart=True)
        yield looper, newNode, client, wallet, newStewardClient, \
              newStewardWallet


@pytest.fixture("module")
def nodeSetWithNodeAddedAfterSomeTxns(txnPoolNodeSet, nodeCreatedAfterSomeTxns):
    looper, newNode, client, wallet, newStewardClient, newStewardWallet = \
        nodeCreatedAfterSomeTxns
    txnPoolNodeSet.append(newNode)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=10))
    looper.run(newStewardClient.ensureConnectedToNodes())
    looper.run(client.ensureConnectedToNodes())
    return looper, newNode, client, wallet, newStewardClient, newStewardWallet


@pytest.fixture("module")
def newNodeCaughtUp(txnPoolNodeSet, nodeSetWithNodeAddedAfterSomeTxns):
    looper, newNode, _, _, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    looper.run(eventually(checkNodeLedgersForEquality, newNode,
                          *txnPoolNodeSet[:4], retryWait=1, timeout=10))
    return newNode
