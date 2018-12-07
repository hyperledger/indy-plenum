import json

import pytest
from plenum.common.util import getMaxFailures

from stp_core.loop.eventually import eventually
from plenum.common.exceptions import MissingSignature
from stp_core.common.log import getlogger
from plenum.common.types import f
from plenum.server.node import Node
from plenum.test import waits
from plenum.test.helper import \
    checkLastClientReqForNode, sdk_signed_random_requests, \
    sdk_send_signed_requests, sdk_json_to_request_object, \
    sdk_get_and_check_replies, sdk_send_random_request

nodeCount = 7

F = getMaxFailures(nodeCount)

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


# noinspection PyIncorrectDocstring
def testReplyWhenRequestAlreadyExecuted(looper, txnPoolNodeSet, sdk_pool_handle,
                                        sdk_wallet_client, sent1):
    """
    When a request has already been executed the previously executed reply
    will be sent again to the client. An acknowledgement will not be sent
    for a repeated request.
    """
    sdk_get_and_check_replies(looper, sent1)
    req = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)
    sdk_get_and_check_replies(looper, [req])
