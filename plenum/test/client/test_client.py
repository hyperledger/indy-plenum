import json

import pytest
from plenum.common.util import getMaxFailures

from stp_core.loop.eventually import eventually
from plenum.common.exceptions import MissingSignature
from stp_core.common.log import getlogger
from plenum.common.constants import OP_FIELD_NAME, REQACK
from plenum.common.types import f
from plenum.server.node import Node
from plenum.test import waits
from plenum.test.helper import \
    checkLastClientReqForNode, getRepliesFromClientInbox, \
    waitForSufficientRepliesForRequests, assertLength, \
    sdk_signed_random_requests, sdk_send_signed_requests, \
    sdk_json_to_request_object

nodeCount = 7

F = getMaxFailures(nodeCount)

whitelist = ['signer not configured so not signing',
             'for EmptySignature',
             'discarding message',
             'found legacy entry',
             'public key from disk',
             'verification key from disk',
             'got error while verifying message']  # warnings

logger = getlogger()


# noinspection PyIncorrectDocstring
def testSendRequestWithoutSignatureFails(looper, txnPoolNodeSet,
                                         sdk_pool_handle, sdk_wallet_client):
    """
    A client request sent without a signature fails with an EmptySignature
    exception
    """

    # remove the client's ability to sign
    requests = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    json_req = json.loads(requests[0])
    json_req['signature'] = None
    request = json.dumps(json_req)
    res = sdk_send_signed_requests(sdk_pool_handle, [request])
    obj_req = sdk_json_to_request_object(res[0][0])

    timeout = waits.expectedClientRequestPropagationTime(nodeCount)

    with pytest.raises(AssertionError):
        for node in txnPoolNodeSet:
            looper.loop.run_until_complete(eventually(
                checkLastClientReqForNode, node, obj_req,
                retryWait=1, timeout=timeout))

    for n in txnPoolNodeSet:
        params = n.spylog.getLastParams(Node.handleInvalidClientMsg)
        ex = params['ex']
        msg, _ = params['wrappedMsg']
        assert isinstance(ex, MissingSignature)
        assert msg.get(f.IDENTIFIER.nm) == obj_req.identifier

        params = n.spylog.getLastParams(Node.discard)
        reason = params["reason"]
        (msg, frm) = params["msg"]
        assert msg == json_req
        assert msg.get(f.IDENTIFIER.nm) == obj_req.identifier
        assert "MissingSignature" in reason


CLI_REQ = pytest.mark.rbft_spec(section="IV", subsection="B", step=1)


@CLI_REQ("A client connects to all the nodes")
def testClientConnectsToAllNodes(client1):
    pass


@CLI_REQ("A client sends a request to all the nodes")
def testRequestFullRoundTrip(replied1, client1):
    pass


# noinspection PyIncorrectDocstring
def testEveryNodeRepliesWithNoFaultyNodes(looper, client1, replied1):
    """
    Every node will send a reply to the client when there are no faulty nodes in
    the system
    """

    def chk():
        receivedReplies = getRepliesFromClientInbox(client1.inBox,
                                                    replied1.reqId)
        print(receivedReplies)
        assert len(receivedReplies) == nodeCount

    looper.run(eventually(chk))


# noinspection PyIncorrectDocstring
def testReplyWhenRequestAlreadyExecuted(looper, txnPoolNodeSet, client1, sent1):
    """
    When a request has already been executed the previously executed reply
    will be sent again to the client. An acknowledgement will not be sent
    for a repeated request.
    """
    waitForSufficientRepliesForRequests(looper, client1, requests=[sent1])

    originalRequestResponsesLen = nodeCount * 2
    duplicateRequestRepliesLen = nodeCount  # for a duplicate request we need to

    message_parts, err_msg = \
        client1.nodestack.prepare_for_sending(sent1, None)

    for part in message_parts:
        client1.nodestack._enqueueIntoAllRemotes(part, None)

    def chk():
        assertLength([response for response in client1.inBox
                      if (response[0].get(f.RESULT.nm) and
                          response[0][f.RESULT.nm][f.REQ_ID.nm] == sent1.reqId) or
                      (response[0].get(OP_FIELD_NAME) == REQACK and
                       response[0].get(f.REQ_ID.nm) == sent1.reqId)],
                     originalRequestResponsesLen + duplicateRequestRepliesLen)

    responseTimeout = waits.expectedTransactionExecutionTime(nodeCount)
    looper.run(eventually(chk, retryWait=1, timeout=responseTimeout))
