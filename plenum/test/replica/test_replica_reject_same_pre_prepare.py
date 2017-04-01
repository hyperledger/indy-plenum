import time

import pytest

from stp_core.loop.eventually import eventually
from plenum.common.log import getlogger
from plenum.common.types import PrePrepare
from plenum.common.util import getMaxFailures
from plenum.test.helper import checkPrePrepareReqSent, \
    checkPrePrepareReqRecvd, \
    checkPrepareReqSent
from plenum.test.helper import sendRandomRequest, checkSufficientRepliesRecvd
from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica

whitelist = ['doing nothing for now',
             'cannot process incoming PRE-PREPARE',
             'InvalidSignature']


logger = getlogger()


# noinspection PyIncorrectDocstring
def testReplicasRejectSamePrePrepareMsg(looper, nodeSet, client1, wallet1):
    """
    Replicas should not accept PRE-PREPARE for view "v" and prepare sequence
    number "n" if it has already accepted a request with view number "v" and
    sequence number "n"

    """
    numOfNodes = 4
    fValue = getMaxFailures(numOfNodes)
    request1 = sendRandomRequest(wallet1, client1)
    result1 = looper.run(
        eventually(checkSufficientRepliesRecvd, client1.inBox,
                   request1.reqId, fValue,
                   retryWait=1, timeout=5))
    logger.debug("request {} gives result {}".format(request1, result1))
    primaryRepl = getPrimaryReplica(nodeSet)
    logger.debug("Primary Replica: {}".format(primaryRepl))
    logger.debug(
        "Decrementing the primary replica's pre-prepare sequence number by "
        "one...")
    primaryRepl.lastPrePrepareSeqNo -= 1
    request2 = sendRandomRequest(wallet1, client1)
    looper.run(eventually(checkPrePrepareReqSent, primaryRepl, request2,
                          retryWait=1, timeout=10))

    nonPrimaryReplicas = getNonPrimaryReplicas(nodeSet)
    logger.debug("Non Primary Replicas: " + str(nonPrimaryReplicas))
    prePrepareReq = PrePrepare(
        primaryRepl.instId,
        primaryRepl.viewNo,
        primaryRepl.lastPrePrepareSeqNo,
        wallet1.defaultId,
        request2.reqId,
        request2.digest,
        time.time()
    )

    logger.debug("""Checking whether all the non primary replicas have received
                the pre-prepare request with same sequence number""")
    looper.run(eventually(checkPrePrepareReqRecvd,
                          nonPrimaryReplicas,
                          prePrepareReq,
                          retryWait=1,
                          timeout=10))
    logger.debug("""Check that none of the non primary replicas didn't send
    any prepare message "
                             in response to the pre-prepare message""")
    for npr in nonPrimaryReplicas:
        with pytest.raises(AssertionError):
            looper.run(eventually(checkPrepareReqSent,
                                  npr,
                                  wallet1.defaultId,
                                  request2.reqId,
                                  retryWait=1,
                                  timeout=10))
