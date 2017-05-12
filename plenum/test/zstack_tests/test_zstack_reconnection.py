import pytest

from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, stopNodes
from plenum.test.test_node import TestNode, ensureElectionsDone

logger = getlogger()


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


@pytest.mark.skip(reason='SOV-1020')
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
    logger.debug('Stopped node {}'.format(nodeToCrash))
    looper.removeProdable(nodeToCrash)
    looper.runFor(1)
    stopNodes([nodeToCrash], looper)
    # TODO Select or create the timeout from 'waits'. Don't use constant.
    looper.run(eventually(checkFlakyConnected, False, retryWait=1, timeout=35))
    looper.runFor(1)
    node = TestNode(nodeToCrash.name, basedirpath=tdirWithPoolTxns, config=tconf,
                    ha=nodeToCrash.nodestack.ha, cliha=nodeToCrash.clientstack.ha)
    looper.add(node)
    txnPoolNodeSet[idxToCrash] = node
    # TODO Select or create the timeout from 'waits'. Don't use constant.
    looper.run(eventually(checkFlakyConnected, True, retryWait=2, timeout=50))
    # TODO Select or create the timeout from 'waits'. Don't use constant.
    ensureElectionsDone(looper, txnPoolNodeSet, retryWait=2, customTimeout=50)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)
    checkNodesSendingCommits(txnPoolNodeSet)
