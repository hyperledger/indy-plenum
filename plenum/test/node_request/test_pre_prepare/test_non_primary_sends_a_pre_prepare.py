import time
import types

import pytest as pytest

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.request import ReqDigest
from plenum.common.types import PrePrepare
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import getNodeSuspicions
from plenum.test.instances.helper import recvdPrePrepare
from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica

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
                firstNpr.lastPrePrepareSeqNo,
                propagated1.identifier,
                propagated1.reqId,
                propagated1.digest,
                time.time())

    ppr = sendPrePrepareFromNonPrimary(firstNpr)

    def chk():
        for r in (primaryReplica, *remainingNpr):
            recvdPps = recvdPrePrepare(r)
            assert len(recvdPps) == 1
            assert recvdPps[0]['pp'][:-1] == ppr[:-1]
            nodeSuspicions = len(getNodeSuspicions(
                r.node, Suspicions.PPR_FRM_NON_PRIMARY.code))
            assert nodeSuspicions == 1

    looper.run(eventually(chk,
                          retryWait=.5, timeout=5))

    # TODO Why is this here? Why would a suspicious PRE-PREPARE from a
    # non-primary warrant a view change? Need more of a story about the scenario
    # looper.run(eventually(checkViewNoForNodes, nodeSet, 1,
    #                       retryWait=1, timeout=20))
