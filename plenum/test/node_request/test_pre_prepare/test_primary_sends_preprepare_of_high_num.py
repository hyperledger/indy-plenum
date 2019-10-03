import pytest

from plenum.common.util import get_utc_epoch
from plenum.server.replica_helper import TPCStat
from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import PrePrepare
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import getNodeSuspicions
from plenum.test.test_node import getPrimaryReplica
from plenum.test import waits
from plenum.test.test_node import getNonPrimaryReplicas

instId = 0


@pytest.mark.skip(reason="SOV-555. Not implemented in replica. Add a check in "
                         "replica to check value of preprepare seq number.")
def testPrePrepareWithHighSeqNo(looper, txnPoolNodeSet, propagated1):
    def chk():
        for r in getNonPrimaryReplicas(txnPoolNodeSet, instId):
            nodeSuspicions = len(getNodeSuspicions(
                r.node, Suspicions.WRONG_PPSEQ_NO.code))
            assert nodeSuspicions == 1

    def checkPreprepare(replica, viewNo, ppSeqNo, req, numOfPrePrepares):
        assert (replica._ordering_service.prePrepares[viewNo, ppSeqNo][0]) == \
               (req.identifier, req.reqId, req.digest)

    primary = getPrimaryReplica(txnPoolNodeSet, instId)
    nonPrimaryReplicas = getNonPrimaryReplicas(txnPoolNodeSet, instId)
    req = propagated1.reqDigest
    primary.doPrePrepare(req)
    timeout = waits.expectedPrePrepareTime(len(txnPoolNodeSet))
    for np in nonPrimaryReplicas:
        looper.run(
            eventually(checkPreprepare, np, primary.viewNo,
                       primary.lastPrePrepareSeqNo - 1, req, 1,
                       retryWait=.5, timeout=timeout))

    newReqDigest = (req.identifier, req.reqId + 1, req.digest)
    incorrectPrePrepareReq = PrePrepare(instId,
                                        primary.viewNo,
                                        primary.lastPrePrepareSeqNo + 2,
                                        *newReqDigest,
                                        get_utc_epoch())
    primary.send(incorrectPrePrepareReq, TPCStat.PrePrepareSent)

    timeout = waits.expectedPrePrepareTime(len(txnPoolNodeSet))
    looper.run(eventually(chk, retryWait=1, timeout=timeout))
