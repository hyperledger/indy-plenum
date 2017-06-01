import types

from stp_core.loop.eventually import eventually
from plenum.common.request import ReqDigest
from plenum.test.helper import sendRandomRequest
from plenum.test.malicious_behaviors_node import delaysPrePrepareProcessing
from plenum.test.test_node import getNonPrimaryReplicas


def testOrderingCase1(looper, nodeSet, up, client1, wallet1):
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
    replica = getNonPrimaryReplicas(nodeSet, instId=0)[0]
    delaysPrePrepareProcessing(replica.node, delay=delay, instId=0)

    def doNotProcessReqDigest(self, rd: ReqDigest):
        pass

    patchedMethod = types.MethodType(doNotProcessReqDigest, replica)
    replica.processRequest = patchedMethod

    def chk(n):
        assert replica.spylog.count(replica.doOrder.__name__) == n

    sendRandomRequest(wallet1, client1)
    timeout = delay - 5
    looper.run(eventually(chk, 0, retryWait=1, timeout=timeout))
    timeout = delay + 5
    looper.run(eventually(chk, 1, retryWait=1, timeout=timeout))
