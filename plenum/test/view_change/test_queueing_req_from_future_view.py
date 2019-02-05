from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.test.delayers import icDelay, vcd_delay
from plenum.test.helper import sdk_send_random_requests, \
    sdk_get_replies, sdk_send_random_and_check
from plenum.test.test_node import get_last_master_non_primary_node

nodeCount = 7

logger = getlogger()


# noinspection PyIncorrectDocstring
def testQueueingReqFromFutureView(delayed_perf_chk, looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client):
    """
    Test if every node queues 3 Phase requests(PRE-PREPARE, PREPARE and COMMIT)
    that come from a view which is greater than the current view.
    - Delay reception and processing of view change messages by a non primary for master instance
       => it starts receiving 3 phase commit messages for next view
    """

    lagging_node = get_last_master_non_primary_node(txnPoolNodeSet)
    old_view_no = lagging_node.viewNo

    # Delay processing of InstanceChange and ViewChangeDone so node stashes
    # 3PC messages
    delay_ic = 60
    lagging_node.nodeIbStasher.delay(icDelay(delay_ic))
    lagging_node.nodeIbStasher.delay(vcd_delay(delay_ic))
    logger.debug('{} will delay its view change'.format(lagging_node))

    def chk_fut_view(is_empty):
        num_stashed = lagging_node.master_replica.stasher.num_stashed_future_view
        if is_empty:
            assert num_stashed == 0
        else:
            assert num_stashed > 0
        return num_stashed

    # No messages queued for future view
    chk_fut_view(is_empty=True)
    logger.debug('{} does not have any messages for future views'
                 .format(lagging_node))

    # Every node except Node A should do a view change
    ensure_view_change(looper,
                       [n for n in txnPoolNodeSet if n != lagging_node],
                       [lagging_node])

    # send more requests that will be queued for the lagged node
    reqs = sdk_send_random_requests(looper, sdk_pool_handle,
                                    sdk_wallet_client, 5)
    l = looper.run(eventually(chk_fut_view, False,
                              retryWait=1))
    logger.debug('{} has {} messages for future views'
                 .format(lagging_node, l))
    sdk_get_replies(looper, reqs)
    # reset delays for the lagging_node node so that it finally makes view
    # change
    lagging_node.reset_delays_and_process_delayeds()

    # Eventually no messages queued for future view
    looper.run(eventually(chk_fut_view, True,
                          retryWait=1, timeout=delay_ic + 10))
    logger.debug('{} exhausted pending messages for future views'
                 .format(lagging_node))

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 2)
