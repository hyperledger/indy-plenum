from stp_core.common.log import getlogger
from plenum.common.messages.node_messages import PrePrepare
from plenum.test.batching_3pc.helper import checkNodesHaveSameRoots
from plenum.test.spy_helpers import getAllArgs
from plenum.test.test_node import getPrimaryReplica, getNonPrimaryReplicas
from plenum.test.view_change.conftest import perf_chk_patched
from plenum.test.helper import sdk_send_random_and_check

logger = getlogger()

pp_delay = 3


def testPrePrepareProcessedInOrder(perf_chk_patched, looper, txnPoolNodeSet,
                                   sdk_pool_handle, sdk_wallet_client):
    """
    A non-primary receives PRE-PREPARE out of order, it receives with ppSeqNo 2
     earlier than it receives the one with ppSeqNo 1 but it stashes the one
     with ppSeqNo 2 and only unstashes it for processing once it has
     processed PRE-PREPARE with ppSeqNo 1
    :return:
    """
    tconf = perf_chk_patched
    pr, otherR = getPrimaryReplica(txnPoolNodeSet, instId=0), \
        getNonPrimaryReplicas(txnPoolNodeSet, instId=0)
    otherNodes = [r.node for r in otherR]
    ppsToDelay = 2
    delayeds = 0
    expectedDelayeds = (len(txnPoolNodeSet) - 1) * ppsToDelay
    delayedPpSeqNos = set()

    def specificPrePrepares(wrappedMsg):
        nonlocal delayeds
        msg, sender = wrappedMsg
        if isinstance(msg, PrePrepare) and delayeds < expectedDelayeds:
            delayeds += 1
            delayedPpSeqNos.add(msg.ppSeqNo)
            logger.debug('ppSeqNo {} would be delayed'.format(msg.ppSeqNo))
            return pp_delay

    for node in otherNodes:
        logger.debug('{} would be delaying reception of some pre-prepares'.
                     format(node))
        node.nodeIbStasher.delay(specificPrePrepares)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                              (ppsToDelay + 1) * tconf.Max3PCBatchSize)

    checkNodesHaveSameRoots(txnPoolNodeSet)

    for r in otherR:
        seqNos = [a['pp'].ppSeqNo for a in getAllArgs(r, r.addToPrePrepares)]
        seqNos.reverse()
        assert sorted(seqNos) == seqNos
