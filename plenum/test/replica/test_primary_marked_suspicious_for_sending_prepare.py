
import pytest

from plenum.test.delayers import cDelay
from stp_core.loop.eventually import eventually
from plenum.common.exceptions import SuspiciousNode
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import getNodeSuspicions
from plenum.test.spy_helpers import getAllArgs
from plenum.test.test_node import TestNode, getNonPrimaryReplicas, \
    getPrimaryReplica

nodeCount = 7


@pytest.fixture(scope="module")
def delay_commits(nodeSet):
    # Delay COMMITs so that ordering is delayed and checks can be made
    for n in nodeSet:
        n.nodeIbStasher.delay(cDelay(5))


def testPrimarySendsAPrepareAndMarkedSuspicious(looper, nodeSet, delay_commits,
                                                preprepared1):
    def sendPrepareFromPrimary(instId):
        primary = getPrimaryReplica(nodeSet, instId)
        viewNo, ppSeqNo = next(iter(primary.sentPrePrepares.keys()))
        ppReq = primary.sentPrePrepares[viewNo, ppSeqNo]
        primary.doPrepare(ppReq)

        def chk():
            for r in getNonPrimaryReplicas(nodeSet, instId):
                l = len([param for param in getAllArgs(r, r.processPrepare)
                         if param['sender'] == primary.name])
                assert l == 1

        looper.run(eventually(chk))

    sendPrepareFromPrimary(0)

    for node in nodeSet:
        if node in getNonPrimaryReplicas(nodeSet, 0):
            frm, reason, code = getAllArgs(node, TestNode.reportSuspiciousNode)
            assert frm == getPrimaryReplica(nodeSet, 0).node.name
            assert isinstance(reason, SuspiciousNode)
            assert len(getNodeSuspicions(node,
                                         Suspicions.PR_FRM_PRIMARY.code)) == 10
