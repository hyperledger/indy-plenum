import pytest

from plenum.common.eventually import eventually
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import TestNode, ensureElectionsDone


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

    npr = [n for n in txnPoolNodeSet if not n.hasPrimary]
    nodeToCrash = npr[0]
    idxToCrash = txnPoolNodeSet.index(nodeToCrash)
    otherNodes = [_ for _ in txnPoolNodeSet if _ != nodeToCrash]

    def checkFlakyConnected(conn=True):
        for node in otherNodes:
            if conn:
                assert nodeToCrash.nodestack.name in node.nodestack.connecteds
            else:
                assert nodeToCrash.nodestack.name not in node.nodestack.connecteds

    checkFlakyConnected(True)
    nodeToCrash.stop()
    looper.removeProdable(nodeToCrash)
    looper.runFor(2)
    looper.run(eventually(checkFlakyConnected, False, retryWait=1, timeout=20))
    looper.runFor(3)
    node = TestNode(nodeToCrash.name, basedirpath=tdirWithPoolTxns, config=tconf,
                    ha=nodeToCrash.nodestack.ha, cliha=nodeToCrash.clientstack.ha)
    looper.add(node)
    txnPoolNodeSet[idxToCrash] = node
    looper.run(eventually(checkFlakyConnected, True, retryWait=2, timeout=50))
    ensureElectionsDone(looper, txnPoolNodeSet, retryWait=2, timeout=50)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)
    checkNodesSendingCommits(txnPoolNodeSet)
