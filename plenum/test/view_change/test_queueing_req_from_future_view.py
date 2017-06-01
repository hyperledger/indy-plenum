from functools import partial

import pytest

from plenum.test.view_change.helper import provoke_and_wait_for_view_change
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.util import getMaxFailures
from plenum.test import waits
from plenum.test.delayers import ppDelay, icDelay
from plenum.test.helper import sendRandomRequest, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import TestReplica, getNonPrimaryReplicas, \
    checkViewChangeInitiatedForNode

nodeCount = 7

logger = getlogger()


# TODO: This test needs to be implemented
# noinspection PyIncorrectDocstring
@pytest.mark.skip(reason='INDY-84. Complete implementation')
def testQueueingReqFromFutureView(delayed_perf_chk, looper, nodeSet, up,
                                  wallet1, client1):
    """
    Test if every node queues 3 Phase requests(PRE-PREPARE, PREPARE and COMMIT)
    that come from a view which is greater than the current view. Slow down
    the primary node of master protocol instance, delay reception and
    processing of view change message by a non primary for master instance so
    that it starts receiving 3 phase commit messages for next view
    """

    nprs = getNonPrimaryReplicas(nodeSet, 0)
    lagging_node = nprs[0].node
    old_view_no = lagging_node.viewNo

    # Delay processing of instance change on a node
    delay_ic = 60
    lagging_node.nodeIbStasher.delay(icDelay(delay_ic))
    logger.debug('{} will delay its view change'.format(lagging_node))

    # Delay processing of PRE-PREPARE from all non primary replicas of master
    # so master's throughput falls and view changes
    delay_pp = 5
    pp_delayer = ppDelay(delay_pp, 0)
    for r in nprs:
        r.node.nodeIbStasher.delay(pp_delayer)

    timeout = waits.expectedTransactionExecutionTime(len(nodeSet)) + delay_pp
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5,
                                        customTimeoutPerReq=timeout)

    def chk_fut_view(view_no, is_empty):
        length = len(lagging_node.msgsForFutureViews.get(view_no, ()))
        if is_empty:
            assert length == 0
        else:
            assert length > 0
        return length

    # No messages queued for future view
    chk_fut_view(old_view_no+1, is_empty=True)
    logger.debug('{} does not have any messages for future views'
                 .format(lagging_node))

    # Every node except Node A should do a view change
    provoke_and_wait_for_view_change(looper,
                                     [n for n in nodeSet if n != lagging_node],
                                     old_view_no + 1,
                                     wallet1, client1)

    for node in nodeSet:
        node.nodeIbStasher.nodelay(pp_delayer)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 3,
                                        customTimeoutPerReq=timeout)

    # Messages queued for future view
    l = chk_fut_view(old_view_no + 1, is_empty=False)
    logger.debug('{} has {} messages for future views'
                 .format(lagging_node, l))

    # Eventually no messages queued for future view
    looper.run(eventually(chk_fut_view, old_view_no + 1, True,
                          retryWait=1, timeout=delay_ic+10))
    logger.debug('{} exhausted pending messages for future views'
                 .format(lagging_node))

    # timeout = waits.expectedPoolViewChangeStartedTimeout(len(nodeSet)-1)
    # # for node in nodeSet:
    # #     if node.name == nodeA.name:
    # #         # Node A's view should not have changed yet
    # #         with pytest.raises(AssertionError):
    # #             looper.run(eventually(partial(
    # #                 checkViewChangeInitiatedForNode, node, 1),
    # #                 retryWait=1,
    # #                 timeout=timeout))
    # #     else:
    # #         looper.run(eventually(
    # #             partial(checkViewChangeInitiatedForNode, node, 1),
    # #             retryWait=1,
    # #             timeout=timeout))
    #
    #
    # # NodeA should not have any pending 3 phase request for a later view
    # for r in nodeA.replicas:  # type: TestReplica
    #     assert len(r.threePhaseMsgsForLaterView) == 0
    #
    # # Reset delays on incoming messages from all nodes
    # for node in nodeSet:
    #     node.nodeIbStasher.nodelay(pp_delayer)
    #
    # # Send one more request
    # sendRandomRequest(wallet1, client1)
    #
    # def checkPending3PhaseReqs():
    #     # Get all replicas that have their primary status decided
    #     reps = [rep for rep in nodeA.replicas if rep.isPrimary is not None]
    #     # At least one replica should have its primary status decided
    #     assert len(reps) > 0
    #     for r in reps:  # type: TestReplica
    #         logger.debug("primary status for replica {} is {}"
    #                       .format(r, r.primaryNames))
    #         assert len(r.threePhaseMsgsForLaterView) > 0
    #
    # # NodeA should now have pending 3 phase request for a later view
    # timeout = waits.expectedPoolViewChangeStartedTimeout(len(nodeSet)) + delayIcA
    # looper.run(eventually(checkPending3PhaseReqs, retryWait=1, timeout=timeout))
