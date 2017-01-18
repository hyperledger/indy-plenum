from plenum.common.eventually import eventually
from plenum.common.log import getlogger
from plenum.common.types import Commit, PrePrepare
from plenum.test.helper import sendRandomRequests, \
    checkSufficientRepliesForRequests, checkLedgerEquality, getPrimaryReplica, \
    checkAllLedgersEqual
from plenum.test.test_node import getNonPrimaryReplicas

nodeCount = 7

logger = getlogger()


def testOrderingCase1(looper, nodeSet, up, client1, wallet1):
    """
    Scenario -> A client sends requests, some nodes delay COMMITs to few
    specific nodes such some nodes achieve commit quorum later for those
    requests compared to other nodes. But all nodes `ORDER` request in the same
    order of ppSeqNos
    https://www.pivotaltracker.com/n/projects/1889887/stories/133655009
    """
    pr, replicas = getPrimaryReplica(nodeSet, instId=0), \
                   getNonPrimaryReplicas(nodeSet, instId=0)
    assert len(replicas) == 6

    rep0 = pr
    rep1 = replicas[0]
    rep2 = replicas[1]
    rep3 = replicas[2]
    rep4 = replicas[3]
    rep5 = replicas[4]
    rep6 = replicas[5]

    node0 = rep0.node
    node1 = rep1.node
    node2 = rep2.node
    node3 = rep3.node
    node4 = rep4.node
    node5 = rep5.node
    node6 = rep6.node

    requests = sendRandomRequests(wallet1, client1, 15)

    ppSeqsToDelay = 5

    delayedPpSeqNos = set()

    def specificCommits(wrappedMsg):
        nonlocal node3, node4, node5
        msg, sender = wrappedMsg
        if isinstance(msg, PrePrepare):
            if len(delayedPpSeqNos) < ppSeqsToDelay:
                delayedPpSeqNos.add(msg.ppSeqNo)
                logger.debug('ppSeqNo {} corresponding to request id {} would '
                             'be delayed'.format(msg.ppSeqNo, msg.reqId))
        if isinstance(msg, Commit) and msg.instId == 0 and \
            sender in (n.name for n in (node3, node4, node5)) and \
                msg.ppSeqNo in delayedPpSeqNos:
            return 3

    for node in (node1, node2):
        logger.debug('{} would be delaying commits'.format(node))
        node.nodeIbStasher.delay(specificCommits)

    checkSufficientRepliesForRequests(looper, client1, requests)

    def ensureSlowNodesHaveAllTxns():
        nonlocal node1, node2
        for node in node1, node2:
            assert len(node.domainLedger) == 15

    looper.run(eventually(ensureSlowNodesHaveAllTxns, retryWait=1, timeout=15))

    checkAllLedgersEqual((n.domainLedger for n in (node0, node3, node4,
                                                   node5, node6)))

    for node in (node1, node2):
        for n in nodeSet:
            if n != node:
                checkLedgerEquality(node.domainLedger, n.domainLedger)

    checkAllLedgersEqual((n.domainLedger for n in nodeSet))
