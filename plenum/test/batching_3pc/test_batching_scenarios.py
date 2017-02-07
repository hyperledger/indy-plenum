from plenum.common.log import getlogger
from plenum.common.types import PrePrepare
from plenum.test.helper import getPrimaryReplica, sendRandomRequests
from plenum.test.spy_helpers import getAllArgs
from plenum.test.test_node import getNonPrimaryReplicas
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

logger = getlogger()


def testPrePrepareProcessedInOrder(tconf, looper, txnPoolNodeSet, wallet1, client):
    """
    A non-primary receives PRE-PREPARE out of order, it receives with ppSeqNo 2
     earlier than it receives the one with ppSeqNo 1 but it stashes the one
     with ppSeqNo 2 and only unstashes it for processing once it has
     processed PRE-PREPARE with ppSeqNo 1
    :return:
    """
    pr, otherR = getPrimaryReplica(txnPoolNodeSet, instId=0), \
                 getNonPrimaryReplicas(txnPoolNodeSet, instId=0)
    primaryNode = pr.node
    otherNodes = [r.node for r in otherR]
    ppsToDelay = 2
    ppDelay = 3
    delayeds = 0
    expectedDelayeds = (len(txnPoolNodeSet) - 1) * ppsToDelay
    delayedPpSeqNos = set()

    def specificPrePrepares(wrappedMsg):
        nonlocal delayeds
        msg, sender = wrappedMsg
        if isinstance(msg, PrePrepare) and delayeds < expectedDelayeds:
            delayeds += 1
            logger.debug('ppSeqNo {} would be delayed'.format(msg.ppSeqNo))
            return ppDelay

    for node in otherNodes:
        logger.debug('{} would be delaying reception of some pre-prepares'.
                     format(node))
        node.nodeIbStasher.delay(specificPrePrepares)

    reqs = sendRandomRequests(wallet1, client, 3*tconf.Max3PCBatchSize)

    looper.runFor(10)
    for r in otherR:
        logger.debug(getAllArgs(r, r.addToPrePrepares))
