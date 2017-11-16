from plenum.test import waits
from plenum.test.delayers import ppDelay, pDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    countDiscarded
from plenum.test.node_catchup.helper import checkNodeDataForEquality
from plenum.test.test_node import getNonPrimaryReplicas, TestReplica
from stp_core.loop.eventually import eventually


def testNonPrimaryRecvs3PhaseMessageOutsideWatermarks(
        chkFreqPatched,
        looper,
        txnPoolNodeSet,
        client1,
        wallet1,
        client1Connected,
        reqs_for_logsize):
    """
    A node is slow in processing PRE-PREPAREs and PREPAREs such that lot of
    requests happen and the slow node has started getting 3 phase messages
    outside of it watermarks. Check that it queues up requests outside watermarks and once it
    has received stable checkpoint it processes more requests. It sends other
    nodes 3 phase messages older than their stable checkpoint so they should
    discard them.
    """
    delay = 15
    instId = 1
    reqsToSend = reqs_for_logsize + 2
    npr = getNonPrimaryReplicas(txnPoolNodeSet, instId)
    slowReplica = npr[0]
    slowNode = slowReplica.node
    slowNode.nodeIbStasher.delay(ppDelay(delay, instId))
    slowNode.nodeIbStasher.delay(pDelay(delay, instId))

    def discardCounts(replicas, pat):
        counts = {}
        for r in replicas:
            counts[r.name] = countDiscarded(r, pat)
        return counts

    oldStashCount = slowReplica.spylog.count(
        TestReplica.stashOutsideWatermarks.__name__)
    oldDiscardCounts = discardCounts([n.replicas[instId] for n in
                                      txnPoolNodeSet if n != slowNode],
                                     'achieved stable checkpoint')

    sendReqsToNodesAndVerifySuffReplies(
        looper, wallet1, client1, reqsToSend)
    timeout = waits.expectedPoolGetReadyTimeout(len(txnPoolNodeSet))
    looper.run(eventually(checkNodeDataForEquality, slowNode,
                          *[_ for _ in txnPoolNodeSet if _ != slowNode],
                          retryWait=1, timeout=timeout))
    newStashCount = slowReplica.spylog.count(
        TestReplica.stashOutsideWatermarks.__name__)
    assert newStashCount > oldStashCount

    def chk():
        counts = discardCounts([n.replicas[instId] for n in
                                txnPoolNodeSet if n != slowNode],
                               'achieved stable checkpoint')
        for nm, count in counts.items():
            assert count > oldDiscardCounts[nm]

    timeout = waits.expectedNodeToNodeMessageDeliveryTime() * \
        len(txnPoolNodeSet) + delay
    looper.run(eventually(chk, retryWait=1, timeout=timeout))
