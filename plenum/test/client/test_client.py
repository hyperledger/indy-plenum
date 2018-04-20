import pytest
from plenum.common.keygen_utils import initRemoteKeys
from plenum.common.util import getMaxFailures

from stp_core.loop.eventually import eventually
from plenum.common.exceptions import MissingSignature
from plenum.common.exceptions import NotConnectedToAny
from stp_core.common.log import getlogger
from plenum.common.types import f
from plenum.server.node import Node
from plenum.test import waits
from plenum.test.helper import randomOperation, \
    checkLastClientReqForNode, sdk_get_and_check_replies, sdk_send_random_request

from plenum.test.test_client import genTestClient

nodeCount = 7

F = getMaxFailures(nodeCount)

logger = getlogger()


# noinspection PyIncorrectDocstring
@pytest.mark.skip(reason='get rid of registry pool')
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
@pytest.mark.skip(reason='get rid of registry pool')
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
