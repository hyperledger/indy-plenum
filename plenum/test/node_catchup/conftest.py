import pytest
from plenum.client.signer import SimpleSigner
from plenum.client.wallet import Wallet
from plenum.common.looper import Looper
from plenum.common.util import randomString
from plenum.test.conftest import getValueFromModule
from plenum.test.eventually import eventually
from plenum.test.helper import TestClient, genHa, \
    sendReqsToNodesAndVerifySuffReplies, checkNodesConnected
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality
from plenum.test.pool_transactions.helper import addNewClient, addNewNode, \
    addNewStewardAndNode, buildPoolClientAndWallet


@pytest.yield_fixture("module")
def nodeCreatedAfterSomeTxns(txnPoolNodeSet, tdirWithPoolTxns,
                             poolTxnStewardData, tconf,
                             allPluginsPath, request):
    with Looper(debug=True) as looper:
        # name, sigseed = poolTxnStewardData
        # stewardSigner = SimpleSigner(seed=sigseed)
        # wallet = Wallet(name)
        # wallet.addSigner(signer=stewardSigner)
        # client = TestClient(name=name, nodeReg=None, ha=genHa(),
        #                     basedirpath=tdirWithPoolTxns)
        client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                                  tdirWithPoolTxns)
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
