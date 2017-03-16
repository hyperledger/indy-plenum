import pytest

from plenum.common.eventually import eventually
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.conftest import tdir
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import TestNode, ensureElectionsDone, \
    getNonPrimaryReplicas


@pytest.fixture(scope="module")
def tconf(conf, tdirWithPoolTxns):
    conf.UseZStack = True
    return conf


def checkNodesSendingCommits(nodeSet):
    for node in nodeSet:
        for r in node.replicas:
            i = r.instId
            commitSenders = [_.voters for _ in r.commits.values()]
            for otherNode in nodeSet:
                if node == otherNode:
                    continue
                otherReplica = otherNode.replicas[i]
                for senders in commitSenders:
                    assert otherReplica.name in senders


def testZStackNodeReconnection(tconf, looper, txnPoolNodeSet, client1, wallet1,
                               tdirWithPoolTxns, client1Connected):
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)
    npr = getNonPrimaryReplicas(txnPoolNodeSet, 0)

    nodeToCrash = npr[0].node
    idxToCrash = txnPoolNodeSet.index(nodeToCrash)
    otherNodes = [_ for _ in txnPoolNodeSet if _ != nodeToCrash]

    def checkAlphaConnected(conn=True):
        for node in otherNodes:
            if conn:
                assert nodeToCrash.nodestack.name in node.nodestack.connecteds
            else:
                assert nodeToCrash.nodestack.name not in node.nodestack.connecteds

    checkAlphaConnected(True)
    nodeToCrash.stop()
    looper.removeProdable(nodeToCrash)
    looper.run(eventually(checkAlphaConnected, False, retryWait=.5, timeout=5))
    looper.runFor(3)
    node = TestNode(nodeToCrash.name, basedirpath=tdirWithPoolTxns, config=tconf,
                    ha=nodeToCrash.nodestack.ha, cliha=nodeToCrash.clientstack.ha)
    looper.add(node)
    txnPoolNodeSet[idxToCrash] = node
    looper.run(eventually(checkAlphaConnected, True, retryWait=2, timeout=50))
    ensureElectionsDone(looper, txnPoolNodeSet, retryWait=2, timeout=50)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)
    checkNodesSendingCommits(txnPoolNodeSet)
