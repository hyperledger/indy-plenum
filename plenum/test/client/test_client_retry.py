import types
from functools import partial

import pytest

from plenum.common.request import Request
from plenum.common.types import Reply, RequestNack
from plenum.test.eventually import eventually, eventuallyAll
from plenum.test.helper import sendRandomRequest, checkReqAck, checkReplyCount

whitelist = ['AlphaC unable to send message', ]


def testClientRetryRequestWhenAckNotReceived(looper, nodeSet, client1,
                                             wallet1, tconf):
    """
    The client gets disconnected from node say Alpha but does not know it.
    It sends request to all nodes including Alpha, expects ACK and REPLY from
    Alpha too, does not get it, so reconnects to Alpha and sends request again
    and gets REPLY
    """
    alpha = nodeSet.Alpha

    r = alpha.clientstack.getRemote(client1.stackName)
    alpha.clientstack.removeRemote(r)
    req = sendRandomRequest(wallet1, client1)

    def chkAcks():
        for node in nodeSet:
            if node != alpha:
                checkReqAck(client1, node, *req.key)
            else:
                with pytest.raises(AssertionError):
                    checkReqAck(client1, node, *req.key)

    looper.run(eventually(chkAcks, retryWait=1, timeout=3))

    looper.run(eventually(checkReplyCount, client1, *req.key, 4, retryWait=1,
                          timeout=tconf.CLIENT_REQACK_TIMEOUT+5))


def testClientRetryRequestWhenReplyNotReceived(looper, nodeSet, client1,
                                               wallet1, tconf):
    """
    A node say Alpha sends ACK but doesn't send REPLY. The connect resends the
    request and gets REPLY
    """

    alpha = nodeSet.Alpha
    skipped = False
    origTrans = alpha.transmitToClient

    def skipReplyOnce(msg, remoteName):
        nonlocal skipped
        if isinstance(msg, Reply) and not skipped:
            skipped = True
            return
        origTrans(msg, remoteName)

    alpha.transmitToClient = skipReplyOnce
    req = sendRandomRequest(wallet1, client1)
    coros = [partial(checkReqAck, client1, node, *req.key) for node in nodeSet]
    looper.run(eventuallyAll(*coros, retryWait=.5, totalTimeout=3))
    looper.run(eventually(checkReplyCount, client1, *req.key, 3, retryWait=1,
                          timeout=3))
    looper.run(eventually(checkReplyCount, client1, *req.key, 4, retryWait=1,
                          timeout=tconf.CLIENT_REPLY_TIMEOUT + 5))


def testClientNotRetryRequestWhenReqnackReceived(looper, nodeSet, client1,
                                                 wallet1, tconf):
    """
    A node sends REQNACK. The client does not resend Request.
    """

    alpha = nodeSet.Alpha
    origProcReq = alpha.processRequest
    origTrans = alpha.transmitToClient

    def nackReq(self, req, frm):
        self.transmitToClient(RequestNack(*req.key, reason="testing"), frm)

    def onlyTransNack(msg, remoteName):
        if not isinstance(msg, RequestNack):
            return
        origTrans(msg, remoteName)

    alpha.clientMsgRouter.routes[Request] = types.MethodType(nackReq, alpha)
    alpha.transmitToClient = onlyTransNack

    totalResends = client1.spylog.count(client1.resendRequests.__name__)
    req = sendRandomRequest(wallet1, client1)
    # Wait till ACK timeout
    looper.runFor(tconf.CLIENT_REQACK_TIMEOUT+1)
    assert client1.spylog.count(client1.resendRequests.__name__) == totalResends
    # Wait till REPLY timeout
    looper.runFor(tconf.CLIENT_REPLY_TIMEOUT - tconf.CLIENT_REQACK_TIMEOUT + 1)
    assert client1.spylog.count(client1.resendRequests.__name__) == totalResends
    looper.run(eventually(checkReplyCount, client1, *req.key, 3, retryWait=1,
                          timeout=3))
    alpha.clientMsgRouter.routes[Request] = origProcReq
    alpha.transmitToClient = origTrans


def testClientNotRetryingRequestAfterMaxTriesDone(looper, nodeSet, client1,
                                                 wallet1, tconf):
    """
    A client sends Request to a node but the node never responds to client.
    The client resends the request but only the number of times defined in its
    configuration and no more
    """
    alpha = nodeSet.Alpha
    origTrans = alpha.transmitToClient

    def dontTransmitReply(msg, remoteName):
        if isinstance(msg, Reply):
            return
        origTrans(msg, remoteName)

    alpha.transmitToClient = dontTransmitReply

    totalResends = client1.spylog.count(client1.resendRequests.__name__)
    req = sendRandomRequest(wallet1, client1)
    # Wait for more than REPLY timeout
    looper.runFor((tconf.CLIENT_MAX_RETRY_REPLY+2)*tconf.CLIENT_REPLY_TIMEOUT+2)
    looper.run(eventually(checkReplyCount, client1, *req.key, 3, retryWait=1,
                          timeout=3))
    assert client1.spylog.count(client1.resendRequests.__name__) == \
        (totalResends + tconf.CLIENT_MAX_RETRY_REPLY)
    assert req.key not in client1.expectingAcksFor
    assert req.key not in client1.expectingRepliesFor
    alpha.processRequest = origTrans
