import pytest
from plenum.common.keygen_utils import initRemoteKeys
from plenum.common.util import getMaxFailures

from stp_core.loop.eventually import eventually
from plenum.common.exceptions import MissingSignature
from plenum.common.exceptions import NotConnectedToAny
from stp_core.common.log import getlogger
from plenum.common.constants import OP_FIELD_NAME, REPLY, REQACK
from plenum.common.types import f
from plenum.server.node import Node
from plenum.test import waits
from plenum.test.helper import randomOperation, \
    checkLastClientReqForNode, getRepliesFromClientInbox, \
    waitForSufficientRepliesForRequests, assertLength

from plenum.test.test_client import genTestClient

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
def testClientShouldNotBeAbleToConnectToNodesNodeStack(pool):
    """
    Client should not be able to connect to nodes in the node's nodestack
    """

    async def go(ctx):
        nodestacksVersion = {k: v.ha for k, v in ctx.nodeset.nodeReg.items()}
        client1, _ = genTestClient(
            nodeReg=nodestacksVersion, tmpdir=ctx.tmpdir)
        for node in ctx.nodeset:
            stack = node.nodestack
            args = (client1.name, stack.name, client1.keys_dir, stack.verhex, True)
            initRemoteKeys(*args)

        ctx.looper.add(client1)
        with pytest.raises(NotConnectedToAny):
            await client1.ensureConnectedToNodes()

    pool.run(go)


# noinspection PyIncorrectDocstring
def testSendRequestWithoutSignatureFails(pool):
    """
    A client request sent without a signature fails with an EmptySignature
    exception
    """

    async def go(ctx):
        client1, wallet = genTestClient(ctx.nodeset, tmpdir=ctx.tmpdir)

        # remove the client's ability to sign
        assert wallet.defaultId

        ctx.looper.add(client1)
        await client1.ensureConnectedToNodes()

        request = wallet.signOp(op=randomOperation())
        request.signature = None
        request = client1.submitReqs(request)[0][0]
        timeout = waits.expectedClientRequestPropagationTime(nodeCount)

        with pytest.raises(AssertionError):
            for node in ctx.nodeset:
                await eventually(
                    checkLastClientReqForNode, node, request,
                    retryWait=1, timeout=timeout)

        for n in ctx.nodeset:
            params = n.spylog.getLastParams(Node.handleInvalidClientMsg)
            ex = params['ex']
            msg, _ = params['wrappedMsg']
            assert isinstance(ex, MissingSignature)
            assert msg.get(f.IDENTIFIER.nm) == request.identifier

            params = n.spylog.getLastParams(Node.discard)
            reason = params["reason"]
            (msg, frm) = params["msg"]
            assert msg == request.as_dict
            assert msg.get(f.IDENTIFIER.nm) == request.identifier
            assert "MissingSignature" in reason

    pool.run(go)


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
