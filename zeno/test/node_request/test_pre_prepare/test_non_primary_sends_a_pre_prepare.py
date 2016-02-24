import types
from functools import partial

import pytest as pytest
from zeno.common.request_types import PrePrepare, ReqDigest
from zeno.test.eventually import eventually
from zeno.test.helper import getPrimaryReplica, getNonPrimaryReplicas, \
    checkViewNoForNodes, getNodeSuspicions

from zeno.common.util import getlogger
from zeno.server.suspicion_codes import Suspicions
from zeno.test.instances.helper import recvdPrePrepare

logger = getlogger()

nodeCount = 7

instId = 0
whitelist = [Suspicions.PPR_FRM_NON_PRIMARY.reason,
             'cannot process incoming PRE-PREPARE']


@pytest.fixture(scope="module")
def setup(nodeSet, up):
    def dontSendPrePrepareRequest(self, reqDigest: ReqDigest):
        logger.debug("EVIL: {} not sending pre-prepare message for request {}".
                     format(self.name, reqDigest))
        return

    pr = getPrimaryReplica(nodeSet, instId)
    evilMethod = types.MethodType(dontSendPrePrepareRequest, pr)
    pr.doPrePrepare = evilMethod


def testNonPrimarySendsAPrePrepare(looper, nodeSet, setup, propagated1):
    primaryReplica = getPrimaryReplica(nodeSet, instId)
    nonPrimaryReplicas = getNonPrimaryReplicas(nodeSet, instId)
    firstNpr = nonPrimaryReplicas[0]
    remainingNpr = nonPrimaryReplicas[1:]

    def sendPrePrepareFromNonPrimary(replica):
        firstNpr.doPrePrepare(propagated1.reqDigest)

        return PrePrepare(
                replica.instId,
                firstNpr.viewNo,
                firstNpr.prePrepareSeqNo,
                propagated1.clientId,
                propagated1.reqId,
                propagated1.digest)

    ppr = sendPrePrepareFromNonPrimary(firstNpr)

    def chk():
        for r in (primaryReplica, *remainingNpr):
            recvdPps = recvdPrePrepare(r)
            assert len(recvdPps) == 1
            assert recvdPps[0]['pp'] == ppr
            nodeSuspicions = len(getNodeSuspicions(
                r.node, Suspicions.PPR_FRM_NON_PRIMARY.code))
            assert nodeSuspicions == 1

    looper.run(eventually(chk,
                          retryWait=.5, timeout=5))
    looper.run(eventually(partial(checkViewNoForNodes, nodeSet, 1), retryWait=1,
                          timeout=20))
