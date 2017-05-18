from plenum.common.types import Nomination
from plenum.test import waits
from plenum.test.delayers import delayerMsgTuple
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.malicious_behaviors_node import slow_non_primary
from plenum.test.test_node import ensureElectionsDone, TestReplica
from plenum.test.view_change.helper import ensure_view_change, \
    check_each_node_reaches_same_end_for_view
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually


logger = getlogger()


def test_nominate_only_advanced(looper, txnPoolNodeSet, client1,
                                wallet1, client1Connected):
    """
    One node lags behind others and has not seen all 3PC messages, hence not
    ordered as many requests as others. But it sends Nomination to others faster
    than others but no node nominates for this lagging node
    """
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 2)
    # Long delay so that election messages reach the node but it is still
    # behind others
    c_delay = 3
    e_delay = 1
    npr = slow_non_primary(txnPoolNodeSet, 0, c_delay)
    lagging_node = npr.node
    logger.debug('{} will be slow by {}'.format(lagging_node, c_delay))
    for node in txnPoolNodeSet:
        if node != lagging_node:
            # This node will get Nominations from the lagging node earlier
            # than Nominations from other nodes
            for o in [n for n in txnPoolNodeSet if n not in (node, lagging_node)]:
                node.nodeIbStasher.delay(delayerMsgTuple(e_delay, Nomination, o.name))

    reqs = sendRandomRequests(wallet1, client1, 10)
    new_view_no = ensure_view_change(looper, txnPoolNodeSet, client1, wallet1)
    timeout = waits.expectedPoolElectionTimeout(len(txnPoolNodeSet)) + len(txnPoolNodeSet)*e_delay
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=timeout)

    def chk():
        nm = npr.name
        for node in txnPoolNodeSet:
            if node != lagging_node:
                assert nm in node.elector.nominations[0]
                assert node.elector.nominations[0][TestReplica.generateName(node.name, 0)][0] != npr.name

    looper.run(eventually(chk))
    looper.run(eventually(check_each_node_reaches_same_end_for_view,
                          txnPoolNodeSet, new_view_no))
    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    waitForSufficientRepliesForRequests(looper, client1, requests=reqs,
                                        customTimeoutPerReq=timeout + c_delay)
    for node in txnPoolNodeSet:
        node.nodeIbStasher.resetDelays()


