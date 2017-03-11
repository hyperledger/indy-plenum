import pytest

from plenum.common.eventually import eventually
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.conftest import tdir
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
    alpha = txnPoolNodeSet[0]

    def checkAlphaConnected(conn=True):
        for node in txnPoolNodeSet[1:]:
            if conn:
                assert alpha.nodestack.name in node.nodestack.connecteds
            else:
                assert alpha.nodestack.name not in node.nodestack.connecteds

    checkAlphaConnected(True)
    alpha.stop()
    looper.removeProdable(alpha)
    looper.run(eventually(checkAlphaConnected, False, retryWait=.5, timeout=5))
    # for n in txnPoolNodeSet[1:]:
    #     print('-----------')
    #     n.nodestack.remotes[alpha.nodestack.name].disconnect()

    looper.runFor(3)
    node = TestNode(alpha.name, basedirpath=tdirWithPoolTxns, config=tconf,
                    ha=alpha.nodestack.ha, cliha=alpha.clientstack.ha)
    looper.add(node)
    txnPoolNodeSet[0] = node
    looper.run(eventually(checkAlphaConnected, True, retryWait=2, timeout=50))
    for n in txnPoolNodeSet[1:]:
        print('>>>>>>>>')
        # n.nodestack.reconnectRemote(n.nodestack.remotes[alpha.nodestack.name])
        # n.nodestack.sendPing(n.nodestack.remotes[alpha.nodestack.name])
    ensureElectionsDone(looper, txnPoolNodeSet, retryWait=2, timeout=50)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)
    checkNodesSendingCommits(txnPoolNodeSet)
