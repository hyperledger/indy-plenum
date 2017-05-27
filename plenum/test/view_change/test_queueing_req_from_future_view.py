from functools import partial

import pytest

from plenum.test.view_change.helper import provoke_and_wait_for_view_change, ensure_view_change, do_vc
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.test.delayers import icDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import getNonPrimaryReplicas

nodeCount = 7

logger = getlogger()


# noinspection PyIncorrectDocstring
def testQueueingReqFromFutureView(delayed_perf_chk, looper, nodeSet, up,
                                  wallet1, client1):
    """
    Test if node queues 3 Phase requests(PRE-PREPARE, PREPARE and COMMIT)
    that come from a view which is greater than the current view. Do a view change and delay
    reception and processing of view change message by a non primary for master instance so
    that it starts receiving 3 phase commit messages for next view
    """
    nprs = getNonPrimaryReplicas(nodeSet, 0)
    lagging_node = nprs[0].node
    old_view_no = lagging_node.viewNo

    # Delay processing of instance change on a node
    delay_ic = 40
    lagging_node.nodeIbStasher.delay(icDelay(delay_ic))
    logger.debug('{} will delay its view change'.format(lagging_node))

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)

    def chk_fut_view(view_no, is_empty):
        length = len(lagging_node.msgsForFutureViews.get(view_no, ()))
        if is_empty:
            assert length == 0
        else:
            assert length > 0
        return length

    # No messages queued for future                                      4
    chk_fut_view(old_view_no+1, is_empty=True)
    logger.debug('{} does not have any messages for future views'
                 .format(lagging_node))

    new_view_no = do_vc(looper, [n for n in nodeSet if n != lagging_node], client1,
                        wallet1, old_view_no)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 2)

    # Messages queued for future view
    l = chk_fut_view(new_view_no, is_empty=False)
    logger.debug('{} has {} messages for future views'
                 .format(lagging_node, l))

    # Eventually no messages queued for future view
    looper.run(eventually(chk_fut_view, new_view_no, True,
                          retryWait=1, timeout=delay_ic+10))
    logger.debug('{} exhausted pending messages for future views'
                 .format(lagging_node))
