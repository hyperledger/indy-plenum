import pytest

from plenum.common.request_types import ReqDigest, PrePrepare
from plenum.server.replica import TPCStat
from plenum.server.suspicion_codes import Suspicions
from plenum.test.eventually import eventually
from plenum.test.helper import getPrimaryReplica, getNonPrimaryReplicas, getNodeSuspicions

instId = 0

@pytest.mark.xfail(reason="Not implemented in replica. "
                          "Add a check in replica to check value of preprepare "
                          "seq number.")
def testPrePrepareWithHighSeqNo(looper, nodeSet, propagated1):
    def chk():
        for r in getNonPrimaryReplicas(nodeSet, instId):
            nodeSuspicions = len(getNodeSuspicions(
                    r.node, Suspicions.WRONG_PPSEQ_NO.code))
            assert nodeSuspicions == 1

    def checkPreprepare(replica, viewNo, ppSeqNo, req, numOfPrePrepares):
        assert (replica.prePrepares[viewNo, ppSeqNo]) == (req.clientId, req.reqId, req.digest)

    primary = getPrimaryReplica(nodeSet, instId)
    nonPrimaryReplicas = getNonPrimaryReplicas(nodeSet, instId)
    req = propagated1.reqDigest
    primary.doPrePrepare(req)
    for np in nonPrimaryReplicas:
        looper.run(
                eventually(checkPreprepare, np, primary.viewNo, primary.prePrepareSeqNo - 1,
                           req, 1,
                           retryWait=.5, timeout=10))

    newReqDigest = ReqDigest(req.clientId, req.reqId + 1, req.digest)
    incorrectPrePrepareReq = PrePrepare(instId,
                               primary.viewNo,
                               primary.prePrepareSeqNo + 2,
                               *newReqDigest)
    primary.send(incorrectPrePrepareReq,TPCStat.PrePrepareSent)
    looper.run(eventually(chk, retryWait=1, timeout=50))
