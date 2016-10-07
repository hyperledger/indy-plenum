import types

import pytest

from plenum.common.types import ReqDigest
from plenum.test.eventually import eventually
from plenum.test.helper import getNonPrimaryReplicas, sendRandomRequest
from plenum.test.malicious_behaviors_node import delaysPrePrepareProcessing


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
    replica = getNonPrimaryReplicas(nodeSet, instId=0)[0]
    delaysPrePrepareProcessing(replica.node, delay=10, instId=0)

    def doNotProcessReqDigest(self, rd: ReqDigest):
        pass

    patchedMethod = types.MethodType(doNotProcessReqDigest, replica)
    replica.processReqDigest = patchedMethod

    def chk(n):
        replica.spylog.count(replica.doOrder.__name__) == n

    sendRandomRequest(wallet1, client1)
    looper.run(eventually(chk, 0, retryWait=1, timeout=5))
    looper.run(eventually(chk, 1, retryWait=1, timeout=15))
