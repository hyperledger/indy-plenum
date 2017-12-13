
import pytest

from plenum.test.delayers import cDelay
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.messages.node_messages import PrePrepare
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import getMaxFailures, get_utc_epoch
from plenum.test import waits
from plenum.test.helper import checkPrePrepareReqSent, \
    checkPrePrepareReqRecvd, \
    checkPrepareReqSent, check_sufficient_replies_received, sendRandomRequest
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
    primaryRepl = getPrimaryReplica(nodeSet, 1)
    logger.debug("Primary Replica: {}".format(primaryRepl))
    nonPrimaryReplicas = getNonPrimaryReplicas(nodeSet, 1)
    logger.debug("Non Primary Replicas: " + str(nonPrimaryReplicas))

    # Delay COMMITs so request is not ordered and checks can be made
    c_delay = 10
    for node in nodeSet:
        node.nodeIbStasher.delay(cDelay(delay=c_delay, instId=1))

    request1 = sendRandomRequest(wallet1, client1)
    for npr in nonPrimaryReplicas:
        looper.run(eventually(checkPrepareReqSent,
                              npr,
                              request1.identifier,
                              request1.reqId,
                              primaryRepl.viewNo,
                              retryWait=1))
    prePrepareReq = primaryRepl.sentPrePrepares[primaryRepl.viewNo,
                                                primaryRepl.lastPrePrepareSeqNo]
    looper.run(eventually(checkPrePrepareReqRecvd,
                          nonPrimaryReplicas,
                          prePrepareReq,
                          retryWait=1))

    # logger.debug("Patching the primary replica's pre-prepare sending method ")
    # orig_method = primaryRepl.sendPrePrepare

    # def patched(self, ppReq):
    #     self.sentPrePrepares[ppReq.viewNo, ppReq.ppSeqNo] = ppReq
    #     ppReq = updateNamedTuple(ppReq, **{f.PP_SEQ_NO.nm: 1})
    #     self.send(ppReq, TPCStat.PrePrepareSent)
    #
    # primaryRepl.sendPrePrepare = types.MethodType(patched, primaryRepl)
    logger.debug(
        "Decrementing the primary replica's pre-prepare sequence number by "
        "one...")
    primaryRepl._lastPrePrepareSeqNo -= 1
    view_no = primaryRepl.viewNo
    request2 = sendRandomRequest(wallet1, client1)
    timeout = waits.expectedPrePrepareTime(len(nodeSet))
    looper.run(eventually(checkPrePrepareReqSent, primaryRepl, request2,
                          retryWait=1, timeout=timeout))

    # Since the node is malicious, it will not be able to process requests due
    # to conflicts in PRE-PREPARE
    primaryRepl.node.stop()
    looper.removeProdable(primaryRepl.node)

    reqIdr = [(request2.identifier, request2.reqId)]
    prePrepareReq = PrePrepare(
        primaryRepl.instId,
        view_no,
        primaryRepl.lastPrePrepareSeqNo,
        get_utc_epoch(),
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

    timeout = waits.expectedTransactionExecutionTime(len(nodeSet)) + c_delay
    result1 = \
        looper.run(eventually(check_sufficient_replies_received,
                              client1, request1.identifier, request1.reqId,
                              retryWait=1, timeout=timeout))
    logger.debug("request {} gives result {}".format(request1, result1))
