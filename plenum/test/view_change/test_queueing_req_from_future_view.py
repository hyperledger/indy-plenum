
import pytest

from plenum.test.view_change.helper import provoke_and_wait_for_view_change, ensure_view_change
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.util import getMaxFailures
from plenum.test import waits
from plenum.test.delayers import ppDelay, icDelay, vcd_delay
from plenum.test.helper import sendRandomRequest, \
    sendReqsToNodesAndVerifySuffReplies, sendRandomRequests, \
    waitForSufficientRepliesForRequests, \
    send_reqs_to_nodes_and_verify_all_replies
from plenum.test.test_node import TestReplica, getNonPrimaryReplicas, \
    checkViewChangeInitiatedForNode, get_last_master_non_primary_node

nodeCount = 7

logger = getlogger()


# noinspection PyIncorrectDocstring
def testQueueingReqFromFutureView(delayed_perf_chk, looper, nodeSet, up,
                                  wallet1, client1):
    """
    Test if every node queues 3 Phase requests(PRE-PREPARE, PREPARE and COMMIT)
    that come from a view which is greater than the current view.
    - Delay reception and processing of view change messages by a non primary for master instance
       => it starts receiving 3 phase commit messages for next view
    """

    lagging_node = get_last_master_non_primary_node(nodeSet)
    old_view_no = lagging_node.viewNo

    # Delay processing of InstanceChange and ViewChangeDone so node stashes
    # 3PC messages
    delay_ic = 60
    lagging_node.nodeIbStasher.delay(icDelay(delay_ic))
    lagging_node.nodeIbStasher.delay(vcd_delay(delay_ic))
    logger.debug('{} will delay its view change'.format(lagging_node))

    def chk_fut_view(view_no, is_empty):
        length = len(lagging_node.msgsForFutureViews.get(view_no, ()))
        if is_empty:
            assert length == 0
        else:
            assert length > 0
        return length

    # No messages queued for future view
    chk_fut_view(old_view_no + 1, is_empty=True)
    logger.debug('{} does not have any messages for future views'
                 .format(lagging_node))

    # Every node except Node A should do a view change
    ensure_view_change(looper,
                       [n for n in nodeSet if n != lagging_node],
                       [lagging_node])

    # send more requests that will be queued for the lagged node
    # sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 3)
    reqs = sendRandomRequests(wallet1, client1, 5)
    l = looper.run(eventually(chk_fut_view, old_view_no + 1, False,
                              retryWait=1))
    logger.debug('{} has {} messages for future views'
                 .format(lagging_node, l))

    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)
    # reset delays for the lagging_node node so that it finally makes view
    # change
    lagging_node.reset_delays_and_process_delayeds()

    # Eventually no messages queued for future view
    looper.run(eventually(chk_fut_view, old_view_no + 1, True,
                          retryWait=1, timeout=delay_ic + 10))
    logger.debug('{} exhausted pending messages for future views'
                 .format(lagging_node))

    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1, 2)
