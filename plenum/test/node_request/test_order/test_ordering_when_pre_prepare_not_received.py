import types

from stp_core.loop.eventually import eventually

from plenum.test import waits
from plenum.test.delayers import ppDelay, pDelay
from plenum.test.helper import sdk_send_random_request
from plenum.test.test_node import getNonPrimaryReplicas


def testOrderingWhenPrePrepareNotReceived(looper, txnPoolNodeSet,
                                          sdk_wallet_client, sdk_pool_handle):
    """
    Send commits but delay pre-prepare and prepares such that enough
    commits are received, now the request should not be ordered until
    pre-prepare is received and ordering should just happen once,
    """
    delay = 10
    non_prim_reps = getNonPrimaryReplicas(txnPoolNodeSet, 0)

    slow_rep = non_prim_reps[0]
    slow_node = slow_rep.node
    slow_node.nodeIbStasher.delay(ppDelay(delay, 0))
    slow_node.nodeIbStasher.delay(pDelay(delay, 0))

    stash_pp = []
    stash_p = []
    orig_pp_method = slow_rep._ordering_service.process_preprepare
    orig_p_method = slow_rep._ordering_service.process_prepare

    def patched_pp(self, msg, sender):
        stash_pp.append((msg, sender))

    def patched_p(self, msg, sender):
        stash_p.append((msg, sender))

    slow_rep._ordering_service.process_preprepare = \
        types.MethodType(patched_pp, slow_rep)
    slow_rep._ordering_service.process_prepare = \
        types.MethodType(patched_p, slow_rep)

    def chk1():
        assert len(slow_rep._ordering_service.commitsWaitingForPrepare) > 0

    sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)
    timeout = waits.expectedPrePrepareTime(len(txnPoolNodeSet)) + delay
    looper.run(eventually(chk1, retryWait=1, timeout=timeout))

    for m, s in stash_pp:
        orig_pp_method(m, s)

    for m, s in stash_p:
        orig_p_method(m, s)

    def chk2():
        assert len(slow_rep._ordering_service.commitsWaitingForPrepare) == 0
        assert slow_rep._ordering_service.spylog.count(slow_rep._ordering_service._do_order.__name__) == 1

    timeout = waits.expectedOrderingTime(len(non_prim_reps) + 1) + 2 * delay
    looper.run(eventually(chk2, retryWait=1, timeout=timeout))
