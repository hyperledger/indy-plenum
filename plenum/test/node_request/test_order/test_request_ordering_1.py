import types

from stp_core.loop.eventually import eventually
from plenum.test.helper import sdk_send_random_request
from plenum.test.malicious_behaviors_node import delaysPrePrepareProcessing
from plenum.test.test_node import getNonPrimaryReplicas


def testOrderingCase1(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    """
    Scenario -> PRE-PREPARE not received by the replica, Request not received
    for ordering by the replica, but received enough commits to start ordering.
    It queues up the request so when a PRE-PREPARE is received or request is
    receievd for ordering, an order can be triggered
    https://www.pivotaltracker.com/story/show/125239401

    Reproducing by - Pick a node with no primary replica, replica ignores
    forwarded request to replica and delay reception of PRE-PREPARE sufficiently
    so that enough COMMITs reach to trigger ordering.
    """
    delay = 10
    replica = getNonPrimaryReplicas(txnPoolNodeSet, instId=0)[0]
    delaysPrePrepareProcessing(replica.node, delay=delay, instId=0)

    def doNotProcessReqDigest(self, _):
        pass

    patchedMethod = types.MethodType(doNotProcessReqDigest, replica)
    replica.processRequest = patchedMethod

    def chk(n):
        assert replica._ordering_service.spylog.count(replica._ordering_service._do_order.__name__) == n

    sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)
    timeout = delay - 5
    looper.run(eventually(chk, 0, retryWait=1, timeout=timeout))
    timeout = delay + 5
    looper.run(eventually(chk, 1, retryWait=1, timeout=timeout))
