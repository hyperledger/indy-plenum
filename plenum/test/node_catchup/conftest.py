import pytest
from plenum.client.signer import SimpleSigner
from plenum.common.looper import Looper
from plenum.common.txn import NEW_STEWARD
from plenum.common.util import randomString
from plenum.test.conftest import getValueFromModule
from plenum.test.eventually import eventually
from plenum.test.helper import TestClient, genHa, \
    sendReqsToNodesAndVerifySuffReplies, checkNodesConnected
from plenum.test.pool_transactions.helper import addNewClient, addNewNode


@pytest.yield_fixture("module")
def nodeSetWithNodeAddedAfterSomeTxns(txnPoolNodeSet, tdirWithPoolTxns,
                                       poolTxnStewardData, txnPoolCliNodeReg,
                                       tconf, allPluginsPath, request):
    with Looper(debug=True) as looper:
        name, sigseed = poolTxnStewardData
        stewardSigner = SimpleSigner(seed=sigseed)
        client = TestClient(name=name, nodeReg=txnPoolCliNodeReg, ha=genHa(),
                            signer=stewardSigner, basedirpath=tdirWithPoolTxns)
        looper.add(client)
        looper.run(client.ensureConnectedToNodes())
        txnCount = getValueFromModule(request, "txnCount", 5)
        sendReqsToNodesAndVerifySuffReplies(looper, client, txnCount,
                                            timeoutPerReq=25)

        newStewardName = randomString()
        newStewardSigner = addNewClient(NEW_STEWARD, looper, client,
                                        newStewardName)
        newStewardClient = TestClient(name=newStewardName,
                                      nodeReg=txnPoolCliNodeReg, ha=genHa(),
                                      signer=newStewardSigner,
                                      basedirpath=tdirWithPoolTxns)
        looper.add(newStewardClient)
        looper.run(newStewardClient.ensureConnectedToNodes())
        newNodeName = "Epsilon"
        newNode = addNewNode(looper, newStewardClient, newNodeName,
                             tdirWithPoolTxns, tconf, allPluginsPath)
        txnPoolNodeSet.append(newNode)
        looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                              timeout=5))
        looper.run(newStewardClient.ensureConnectedToNodes())
        looper.run(client.ensureConnectedToNodes())
        yield looper, newNode, client, newStewardClient
