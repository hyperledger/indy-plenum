import time

import pytest

from plenum.test.spy_helpers import getAllArgs, getAllReturnVals
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.messages.node_messages import PrePrepare
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import getMaxFailures
from plenum.test import waits
from plenum.test.helper import checkPrePrepareReqSent, \
    checkPrePrepareReqRecvd, \
    checkPrepareReqSent

from plenum.test.helper import sendRandomRequest, checkSufficientRepliesReceived
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
    timeout = waits.expectedReqAckQuorumTime()
    result1 = looper.run(
        eventually(checkSufficientRepliesReceived, client1.inBox,
                   request1.reqId, fValue,
                   retryWait=1, timeout=timeout))
    logger.debug("request {} gives result {}".format(request1, result1))
    primaryRepl = getPrimaryReplica(nodeSet)
    logger.debug("Primary Replica: {}".format(primaryRepl))
    logger.debug(
        "Decrementing the primary replica's pre-prepare sequence number by "
        "one...")
    primaryRepl._lastPrePrepareSeqNo -= 1
    view_no = primaryRepl.viewNo
    request2 = sendRandomRequest(wallet1, client1)
    timeout = waits.expectedPrePrepareTime(len(nodeSet))
    looper.run(eventually(checkPrePrepareReqSent, primaryRepl, request2,
                          retryWait=1, timeout=timeout))

    nonPrimaryReplicas = getNonPrimaryReplicas(nodeSet)
    logger.debug("Non Primary Replicas: " + str(nonPrimaryReplicas))
    reqIdr = [(request2.identifier, request2.reqId)]
    prePrepareReq = PrePrepare(
        primaryRepl.instId,
        primaryRepl.viewNo,
        primaryRepl.lastPrePrepareSeqNo,
        time.time(),
        reqIdr,
        1,
        primaryRepl.batchDigest([request2]),
        DOMAIN_LEDGER_ID,
        primaryRepl.stateRootHash(DOMAIN_LEDGER_ID),
        primaryRepl.txnRootHash(DOMAIN_LEDGER_ID)
    )

    logger.debug("""Checking whether all the non primary replicas have received
                the pre-prepare request with same sequence number""")
    timeout = waits.expectedPrePrepareTime(len(nodeSet))
    looper.run(eventually(checkPrePrepareReqRecvd,
                          nonPrimaryReplicas,
                          prePrepareReq,
                          retryWait=1,
                          timeout=timeout))
    logger.debug("""Check that none of the non primary replicas didn't send
    any prepare message "
                             in response to the pre-prepare message""")
    timeout = waits.expectedPrepareTime(len(nodeSet))
    looper.runFor(timeout)  # expect prepare processing timeout

    # check if prepares have not been sent
    for npr in nonPrimaryReplicas:
        with pytest.raises(AssertionError):
            looper.run(eventually(checkPrepareReqSent,
                                  npr,
                                  request2.identifier,
                                  request2.reqId,
                                  view_no,
                                  retryWait=1,
                                  timeout=timeout))
