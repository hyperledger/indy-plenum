import logging

import pytest
from plenum.common.request_types import PrePrepare
from plenum.test.eventually import eventually

from plenum.common.util import getMaxFailures
from plenum.test.helper import checkPrePrepareReqSent, checkPrePrepareReqRecvd, \
    checkPrepareReqSent
from plenum.test.helper import sendRandomRequest, checkSufficientRepliesRecvd, \
    getPrimaryReplica, getNonPrimaryReplicas

whitelist = ['doing nothing for now',
             'cannot process incoming PRE-PREPARE',
             'InvalidSignature']


# noinspection PyIncorrectDocstring
def testReplicasRejectSamePrePrepareMsg(looper, nodeSet, client1):
    """
    Replicas should not accept PRE-PREPARE for view "v" and prepare sequence
    number "n" if it has already accepted a request with view number "v" and
    sequence number "n"

    """
    numOfNodes = 4
    fValue = getMaxFailures(numOfNodes)
    request1 = sendRandomRequest(client1)
    result1 = looper.run(
        eventually(checkSufficientRepliesRecvd, client1.inBox,
                   request1.reqId, fValue,
                   retryWait=1, timeout=5))
    logging.debug("request {} gives result {}".format(request1, result1))
    primaryRepl = getPrimaryReplica(nodeSet)
    logging.debug("Primary Replica: {}".format(primaryRepl))
    logging.debug(
        "Decrementing the primary replica's pre-prepare sequence number by one...")
    primaryRepl.prePrepareSeqNo -= 1
    request2 = sendRandomRequest(client1)
    looper.run(eventually(checkPrePrepareReqSent, primaryRepl, request2,
                          retryWait=1, timeout=10))

    nonPrimaryReplicas = getNonPrimaryReplicas(nodeSet)
    logging.debug("Non Primary Replicas: " + str(nonPrimaryReplicas))
    prePrepareReq = PrePrepare(
            primaryRepl.instId,
            primaryRepl.viewNo,
            primaryRepl.prePrepareSeqNo,
            client1.defaultIdentifier,
            request2.reqId,
            request2.digest)

    logging.debug("""Checking whether all the non primary replicas have received
                the pre-prepare request with same sequence number""")
    looper.run(eventually(checkPrePrepareReqRecvd,
                          nonPrimaryReplicas,
                          prePrepareReq,
                          retryWait=1,
                          timeout=10))
    logging.debug("""Check that none of the non primary replicas didn't send any prepare message "
                             in response to the pre-prepare message""")
    for npr in nonPrimaryReplicas:
        with pytest.raises(AssertionError):
            looper.run(eventually(checkPrepareReqSent,
                                  npr,
                                  client1.clientId,
                                  request2.reqId,
                                  retryWait=1,
                                  timeout=10))
