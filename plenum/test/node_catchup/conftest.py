import pytest
from plenum.client.signer import SimpleSigner
from plenum.common.looper import Looper
from plenum.common.util import randomString
from plenum.test.conftest import getValueFromModule
from plenum.test.eventually import eventually
from plenum.test.helper import TestClient, genHa, \
    sendReqsToNodesAndVerifySuffReplies, checkNodesConnected
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality
from plenum.test.pool_transactions.helper import addNewClient, addNewNode, \
    addNewStewardAndNode


@pytest.yield_fixture("module")
def nodeCreatedAfterSomeTxns(txnPoolNodeSet, tdirWithPoolTxns,
                                       poolTxnStewardData, txnPoolCliNodeReg,
                                       tconf, allPluginsPath, request):
    with Looper(debug=True) as looper:
        name, sigseed = poolTxnStewardData
        stewardSigner = SimpleSigner(seed=sigseed)
        client = TestClient(name=name, nodeReg=None, ha=genHa(),
                            signer=stewardSigner, basedirpath=tdirWithPoolTxns)
        looper.add(client)
        looper.run(client.ensureConnectedToNodes())
        txnCount = getValueFromModule(request, "txnCount", 5)
        sendReqsToNodesAndVerifySuffReplies(looper, client, txnCount,
                                            timeoutPerReq=25)

        newStewardName = randomString()
        newNodeName = "Epsilon"
        newStewardClient, newNode = addNewStewardAndNode(looper, client,
                                                         newStewardName,
                                                         newNodeName,
                                                         tdirWithPoolTxns,
                                                         tconf,
                                                         allPluginsPath=allPluginsPath,
                                                         autoStart=True)
        yield looper, newNode, client, newStewardClient


@pytest.fixture("module")
def nodeSetWithNodeAddedAfterSomeTxns(txnPoolNodeSet, nodeCreatedAfterSomeTxns):
    looper, newNode, client, newStewardClient = nodeCreatedAfterSomeTxns
    txnPoolNodeSet.append(newNode)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=5))
    looper.run(newStewardClient.ensureConnectedToNodes())
    looper.run(client.ensureConnectedToNodes())
    return looper, newNode, client, newStewardClient


@pytest.fixture("module")
def newNodeCaughtUp(txnPoolNodeSet, nodeSetWithNodeAddedAfterSomeTxns):
    looper, newNode, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    looper.run(eventually(checkNodeLedgersForEquality, newNode,
                          *txnPoolNodeSet[:4], retryWait=1, timeout=5))
    return newNode
