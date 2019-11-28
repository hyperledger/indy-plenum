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
    checkPrepareReqSent, sdk_send_random_requests, \
    sdk_json_to_request_object, sdk_get_replies, init_discarded
from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica

whitelist = ['doing nothing for now',
             'cannot process incoming PRE-PREPARE',
             'InvalidSignature']

logger = getlogger()


# noinspection PyIncorrectDocstring
def testReplicasRejectSamePrePrepareMsg(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    """
    Replicas should not accept PRE-PREPARE for view "v" and prepare sequence
    number "n" if it has already accepted a request with view number "v" and
    sequence number "n"

    """
    numOfNodes = 4
    fValue = getMaxFailures(numOfNodes)
    primaryRepl = getPrimaryReplica(txnPoolNodeSet, 1)
    logger.debug("Primary Replica: {}".format(primaryRepl))
    nonPrimaryReplicas = getNonPrimaryReplicas(txnPoolNodeSet, 1)
    logger.debug("Non Primary Replicas: " + str(nonPrimaryReplicas))

    # Delay COMMITs so request is not ordered and checks can be made
    c_delay = 10
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(cDelay(delay=c_delay, instId=1))

    req1 = sdk_send_random_requests(looper,
                                    sdk_pool_handle,
                                    sdk_wallet_client,
                                    1)[0]
    request1 = sdk_json_to_request_object(req1[0])
    for npr in nonPrimaryReplicas:
        looper.run(eventually(checkPrepareReqSent,
                              npr,
                              request1.key,
                              primaryRepl.viewNo,
                              retryWait=1))
    prePrepareReq = primaryRepl._ordering_service.sent_preprepares[primaryRepl.viewNo,
                                                primaryRepl.lastPrePrepareSeqNo]
    looper.run(eventually(checkPrePrepareReqRecvd,
                          nonPrimaryReplicas,
                          prePrepareReq,
                          retryWait=1))

    # logger.debug("Patching the primary replica's pre-prepare sending method ")
    # orig_method = primaryRepl.sendPrePrepare

    # def patched(self, ppReq):
    #     self._ordering_service.sent_preprepares[ppReq.viewNo, ppReq.ppSeqNo] = ppReq
    #     ppReq = updateNamedTuple(ppReq, **{f.PP_SEQ_NO.nm: 1})
    #     self.send(ppReq, TPCStat.PrePrepareSent)
    #
    # primaryRepl.sendPrePrepare = types.MethodType(patched, primaryRepl)
    logger.debug(
        "Decrementing the primary replica's pre-prepare sequence number by "
        "one...")
    primaryRepl._ordering_service._lastPrePrepareSeqNo -= 1
    view_no = primaryRepl.viewNo
    request2 = sdk_json_to_request_object(
        sdk_send_random_requests(looper,
                                 sdk_pool_handle,
                                 sdk_wallet_client,
                                 1)[0][0])
    timeout = waits.expectedPrePrepareTime(len(txnPoolNodeSet))
    looper.run(eventually(checkPrePrepareReqSent, primaryRepl, request2,
                          retryWait=1, timeout=timeout))

    # Since the node is malicious, it will not be able to process requests due
    # to conflicts in PRE-PREPARE
    primaryRepl.node.stop()
    looper.removeProdable(primaryRepl.node)

    reqIdr = [request2.digest]
    tm = get_utc_epoch()
    prePrepareReq = PrePrepare(
        primaryRepl.instId,
        view_no,
        primaryRepl.lastPrePrepareSeqNo,
        tm,
        reqIdr,
        init_discarded(),
        primaryRepl._ordering_service.generate_pp_digest([request2.digest], view_no, tm),
        DOMAIN_LEDGER_ID,
        primaryRepl._ordering_service.get_state_root_hash(DOMAIN_LEDGER_ID),
        primaryRepl._ordering_service.get_txn_root_hash(DOMAIN_LEDGER_ID),
        0,
        True
    )

    logger.debug("""Checking whether all the non primary replicas have received
                the pre-prepare request with same sequence number""")
    timeout = waits.expectedPrePrepareTime(len(txnPoolNodeSet))
    looper.run(eventually(checkPrePrepareReqRecvd,
                          nonPrimaryReplicas,
                          prePrepareReq,
                          retryWait=1,
                          timeout=timeout))
    logger.debug("""Check that none of the non primary replicas didn't send
    any prepare message "
                             in response to the pre-prepare message""")
    timeout = waits.expectedPrepareTime(len(txnPoolNodeSet))
    looper.runFor(timeout)  # expect prepare processing timeout

    # check if prepares have not been sent
    for npr in nonPrimaryReplicas:
        with pytest.raises(AssertionError):
            looper.run(eventually(checkPrepareReqSent,
                                  npr,
                                  request2.key,
                                  view_no,
                                  retryWait=1,
                                  timeout=timeout))

    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)) + c_delay
    result1 = sdk_get_replies(looper, [req1])[0][1]
    logger.debug("request {} gives result {}".format(request1, result1))
