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
def delay_commits(txnPoolNodeSet):
    # Delay COMMITs so that ordering is delayed and checks can be made
    for n in txnPoolNodeSet:
        n.nodeIbStasher.delay(cDelay(5))


def testPrimarySendsAPrepareAndMarkedSuspicious(looper, txnPoolNodeSet, delay_commits,
                                                preprepared1):
    def sendPrepareFromPrimary(instId):
        primary = getPrimaryReplica(txnPoolNodeSet, instId)
        viewNo, ppSeqNo = next(iter(primary.sentPrePrepares.keys()))
        ppReq = primary.sentPrePrepares[viewNo, ppSeqNo]
        primary.doPrepare(ppReq)

        def chk():
            for r in getNonPrimaryReplicas(txnPoolNodeSet, instId):
                l = len([param for param in getAllArgs(r, r.processPrepare)
                         if param['prepare'].frm_replica == primary.name])
                assert l == 1

        looper.run(eventually(chk))

    sendPrepareFromPrimary(0)

    for node in txnPoolNodeSet:
        if node in getNonPrimaryReplicas(txnPoolNodeSet, 0):
            frm, reason, code = getAllArgs(node, TestNode.reportSuspiciousNode)
            assert frm == getPrimaryReplica(txnPoolNodeSet, 0).node.name
            assert isinstance(reason, SuspiciousNode)
            assert len(getNodeSuspicions(node,
                                         Suspicions.PR_FRM_PRIMARY.code)) == 10
