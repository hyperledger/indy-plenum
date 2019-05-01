import types

import pytest as pytest

from plenum.common.messages.node_messages import PrePrepare
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.types import f
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import compareNamedTuple
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import getNodeSuspicions
from plenum.test import waits
from plenum.test.instances.helper import recvd_pre_prepares
from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica

logger = getlogger()

nodeCount = 7

instId = 0
whitelist = [Suspicions.PPR_FRM_NON_PRIMARY.reason,
             'cannot process incoming PRE-PREPARE']


@pytest.fixture(scope="module")
def setup(txnPoolNodeSet):
    def dontSendPrePrepareRequest(self, pp_req: PrePrepare):
        logger.debug("EVIL: {} not sending pre-prepare message for request {}".
                     format(self.name, pp_req))
        return

    pr = getPrimaryReplica(txnPoolNodeSet, instId)
    evilMethod = types.MethodType(dontSendPrePrepareRequest, pr)
    pr.sendPrePrepare = evilMethod


def testNonPrimarySendsAPrePrepare(looper, txnPoolNodeSet, setup, propagated1):
    nonPrimaryReplicas = getNonPrimaryReplicas(txnPoolNodeSet, instId)
    firstNpr = nonPrimaryReplicas[0]
    remainingNpr = nonPrimaryReplicas[1:]

    def sendPrePrepareFromNonPrimary():
        firstNpr.requestQueues[DOMAIN_LEDGER_ID].add(propagated1.key)
        ppReq = firstNpr.create_3pc_batch(DOMAIN_LEDGER_ID)
        firstNpr.sendPrePrepare(ppReq)
        return ppReq

    ppr = sendPrePrepareFromNonPrimary()

    def chk():
        for r in remainingNpr:
            recvdPps = recvd_pre_prepares(r)
            assert len(recvdPps) == 1
            assert compareNamedTuple(recvdPps[0], ppr,
                                     f.DIGEST.nm, f.STATE_ROOT.nm,
                                     f.TXN_ROOT.nm)
            nodeSuspicions = len(getNodeSuspicions(
                r.node, Suspicions.PPR_FRM_NON_PRIMARY.code))
            assert nodeSuspicions == 1

    timeout = waits.expectedClientRequestPropagationTime(len(txnPoolNodeSet))
    looper.run(eventually(chk,
                          retryWait=.5, timeout=timeout))

    # TODO Why is this here? Why would a suspicious PRE-PREPARE from a
    # non-primary warrant a view change? Need more of a story about the scenario
    # looper.run(eventually(checkViewNoForNodes, nodeSet, 1,
    #                       retryWait=1, timeout=20))
