import pytest

from plenum.client.signer import SimpleSigner
from plenum.common.looper import Looper
from plenum.common.txn import NEW_STEWARD
from plenum.common.util import randomString
from plenum.test.eventually import eventually
from plenum.test.helper import genHa, checkNodesConnected, \
    TestClient, sendReqsToNodesAndVerifySuffReplies, sendRandomRequests
from plenum.test.pool_transactions.helper import addNewClient, addNewNode
from plenum.test.node_catchup.helper import checkNodeLedgersForEqualSize, \
    ensureNewNodeConnectedClient


@pytest.yield_fixture("module")
def nodeSetWithNodeAddedAfterSomeTxns(txnPoolNodeSet, tdirWithPoolTxns,
                                       poolTxnStewardData, txnPoolCliNodeReg,
                                       tconf):
    with Looper(debug=True) as looper:
        name, pkseed, sigseed = poolTxnStewardData
        stewardSigner = SimpleSigner(seed=sigseed)
        client = TestClient(name=name, nodeReg=txnPoolCliNodeReg, ha=genHa(),
                            signer=stewardSigner, basedirpath=tdirWithPoolTxns)
        looper.add(client)
        looper.run(client.ensureConnectedToNodes())
        sendReqsToNodesAndVerifySuffReplies(looper, client, 5)

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
                             tdirWithPoolTxns, tconf)
        txnPoolNodeSet.append(newNode)
        looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                              timeout=5))
        # ensureNewNodeConnectedClient(looper, client, newNode)
        looper.run(newStewardClient.ensureConnectedToNodes())
        looper.run(client.ensureConnectedToNodes())
        yield looper, newNode, client, newStewardClient


def testNewNodeCatchup(txnPoolNodeSet, nodeSetWithNodeAddedAfterSomeTxns):
    """
    A new node that joins after some transactions should eventually get
    those transactions.
    TODO: Test correct statuses are exchanged
    TODO: Test correct consistency proofs are generated
    :return:
    """
    looper, newNode, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    looper.run(eventually(checkNodeLedgersForEqualSize, newNode,
                          *txnPoolNodeSet[:4], retryWait=1, timeout=5))


def testNodeCatchupAfterRestart(txnPoolNodeSet,
                                nodeSetWithNodeAddedAfterSomeTxns):
    """
    A node that restarts after some transactions should eventually get the
    transactions which happened while it was down
    :return:
    """

    looper, newNode, _, client = nodeSetWithNodeAddedAfterSomeTxns
    newNode.stop()
    sendReqsToNodesAndVerifySuffReplies(looper, client, 5)
    newNode.start(looper.loop)
    looper.run(eventually(checkNodeLedgersForEqualSize, newNode,
                          *txnPoolNodeSet[:4], retryWait=1, timeout=5))


@pytest.mark.skipif(True, reason="failing due to bug "
                                 "https://www.pivotaltracker.com/story/show/121842767")
def testNodeDoesNotParticipateUntilCaughtUp(txnPoolNodeSet,
                                            nodeSetWithNodeAddedAfterSomeTxns):
    """
    A new node that joins after some transactions should stash new transactions
    until it has caught up
    :return:
    """
    looper, newNode, _, client = nodeSetWithNodeAddedAfterSomeTxns
    sendReqsToNodesAndVerifySuffReplies(looper, client, 5)

    for node in txnPoolNodeSet[:4]:
        for replica in node.replicas:
            for commit in replica.commits.values():
                assert newNode.name not in commit.voters
            for prepare in replica.prepares.values():
                assert newNode.name not in prepare.voters
