from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.messages.node_messages import PrePrepare, Commit
from plenum.test.helper import sdk_send_random_requests, sdk_get_and_check_replies
from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica
from plenum.test import waits

nodeCount = 7

logger = getlogger()


def testOrderingCase2(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    """
    Scenario -> A client sends requests, some nodes delay COMMITs to few
    specific nodes such some nodes achieve commit quorum later for those
    requests compared to other nodes. But all nodes `ORDER` request in the same
    order of ppSeqNos
    https://www.pivotaltracker.com/n/projects/1889887/stories/133655009
    """
    pr, replicas = getPrimaryReplica(txnPoolNodeSet, instId=0), \
                   getNonPrimaryReplicas(txnPoolNodeSet, instId=0)
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

    ppSeqsToDelay = 5
    commitDelay = 3  # delay each COMMIT by this number of seconds
    delayedPpSeqNos = set()

    requestCount = 10

    def specificCommits(wrappedMsg):
        nonlocal node3, node4, node5
        msg, sender = wrappedMsg
        if isinstance(msg, PrePrepare):
            if len(delayedPpSeqNos) < ppSeqsToDelay:
                delayedPpSeqNos.add(msg.ppSeqNo)
                logger.debug('ppSeqNo {} be delayed'.format(msg.ppSeqNo))
        if isinstance(msg, Commit) and msg.instId == 0 and \
                sender in (n.name for n in (node3, node4, node5)) and \
                msg.ppSeqNo in delayedPpSeqNos:
            return commitDelay

    for node in (node1, node2):
        logger.debug('{} would be delaying commits'.format(node))
        node.nodeIbStasher.delay(specificCommits)

    sdk_reqs = sdk_send_random_requests(looper, sdk_pool_handle,
                                        sdk_wallet_client, requestCount)

    timeout = waits.expectedPoolGetReadyTimeout(len(txnPoolNodeSet))

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=timeout)

    sdk_get_and_check_replies(looper, sdk_reqs)
