import pytest
from plenum.common.exceptions import EmptySignature
from plenum.common.txn import REPLY
from raet.raeting import AutoMode
from plenum.test.eventually import eventually

from plenum.common.util import getMaxFailures
from plenum.server.node import Node
from plenum.test.helper import NotConnectedToAny
from plenum.test.helper import TestNodeSet, randomOperation, \
    checkLastClientReqForNode, \
    getRepliesFromClientInbox
from plenum.test.helper import checkResponseCorrectnessFromNodes
from plenum.test.helper import sendRandomRequest, genTestClient, \
    checkSufficientRepliesRecvd, assertLength

nodeCount = 7

F = getMaxFailures(nodeCount)

whitelist = ['signer not configured so not signing',
             'for EmptySignature',
             'discarding message',
             'found legacy entry']  # warnings


# noinspection PyIncorrectDocstring
def testGeneratedRequestSequencing(tdir_for_func):
    """
    Request ids must be generated in an increasing order
    """
    with TestNodeSet(count=4, tmpdir=tdir_for_func) as nodeSet:
        cli = genTestClient(nodeSet, tmpdir=tdir_for_func)
        operation = randomOperation()

        request = cli.createRequest(operation)
        assert request.reqId == 1

        request = cli.createRequest(operation)
        assert request.reqId == 2

        request = cli.createRequest(randomOperation())
        assert request.reqId == 3

        cli2 = genTestClient(nodeSet, tmpdir=tdir_for_func)

        request = cli2.createRequest(operation)
        assert request.reqId == 1


# noinspection PyIncorrectDocstring
def testClientShouldNotBeAbleToConnectToNodesNodeStack(pool):
    """
    Client should not be able to connect to nodes in the node's nodestack
    """

    async def go(ctx):
        for n in ctx.nodeset:
            n.nodestack.keep.auto = AutoMode.never

        nodestacksVersion = {k: v.ha for k, v in ctx.nodeset.nodeReg.items()}
        client1 = genTestClient(nodeReg=nodestacksVersion, tmpdir=ctx.tmpdir)
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
        client1 = genTestClient(ctx.nodeset, tmpdir=ctx.tmpdir)

        # remove the client's ability to sign
        assert client1.signer
        client1.signer = None
        assert not client1.signer

        ctx.looper.add(client1)
        await client1.ensureConnectedToNodes()

        operation = randomOperation()
        request = client1.submit(operation)[0]

        with pytest.raises(AssertionError):
            for node in ctx.nodeset:
                await eventually(
                        checkLastClientReqForNode, node, request,
                        retryWait=1, timeout=10)

        for n in ctx.nodeset:
            params = n.spylog.getLastParams(Node.reportSuspiciousClient)
            frm = params['clientName']
            reason = params['reason']

            assert frm == client1.name
            assert isinstance(reason, EmptySignature)

            params = n.spylog.getLastParams(Node.discard)
            reason = params["reason"]
            (msg, frm) = params["msg"]
            assert msg == request.__dict__
            assert frm == client1.name
            assert isinstance(reason, EmptySignature)

    pool.run(go)


CLI_REQ = pytest.mark.rbft_spec(section="IV", subsection="B", step=1)


@CLI_REQ("A client connects to all the nodes")
def testClientConnectsToAllNodes(client1):
    pass


@CLI_REQ("A client sends a request to all the nodes")
def testRequestFullRoundTrip(replied1):
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
        assert len(receivedReplies) == nodeCount

    looper.run(eventually(chk))


# noinspection PyIncorrectDocstring
def testReplyWhenRepliesFromAllNodesAreSame(looper, client1):
    """
    When there are not faulty nodes, the client must get a reply from all the
    nodes.
    """
    request = sendRandomRequest(client1)
    looper.run(
            eventually(assertLength, client1.inBox,
                       2 * nodeCount * request.reqId,
                       retryWait=.25, timeout=15))
    checkResponseCorrectnessFromNodes(client1.inBox, request.reqId, F)


# noinspection PyIncorrectDocstring
def testReplyWhenRepliesFromExactlyFPlusOneNodesAreSame(looper, client1):
    """
    When only :math:`2f+1` replies from the nodes are matching, the client
    would accept the reply
    """
    request = sendRandomRequest(client1)
    # exactly f + 1 => (3) nodes have correct responses
    # modify some (numOfResponses of type REPLY - (f + 1)) => 4 responses to
    # have a different operations
    looper.run(
            eventually(assertLength, client1.inBox,
                       2 * nodeCount * request.reqId,
                       retryWait=.25, timeout=15))

    replies = (msg for msg, frm in client1.inBox
               if msg['op'] == REPLY and
               msg['reqId'] == request.reqId)

    # change two responses to something different
    for i in range(2):
        msg = next(replies)
        msg['result']['txnId'] = str(i) + "Some random id"

    checkResponseCorrectnessFromNodes(client1.inBox, request.reqId, F)


# noinspection PyIncorrectDocstring
def testReplyWhenRequestAlreadyExecuted(looper, nodeSet, client1, sent1):
    """
    When a request has already been executed the previously executed reply
    will be sent again to the client. An acknowledgement will not be sent
    for a repeated request.
    """
    # Since view no is always zero in the current setup
    looper.run(eventually(checkSufficientRepliesRecvd,
                          client1.inBox,
                          sent1.reqId,
                          2,
                          retryWait=.25,
                          timeout=5))
    orignalRquestResponsesLen = nodeCount * 2
    duplicateRequestRepliesLen = nodeCount  # for a duplicate request we need to
    #  send reply only not any ACK.
    client1._enqueueIntoAllRemotes(sent1)
    # Since view no is always zero in the current setup
    looper.run(eventually(
            lambda: assertLength([response for response in client1.inBox
                                  if response[0]['reqId'] == sent1.reqId],
                                 orignalRquestResponsesLen +
                                 duplicateRequestRepliesLen),
            retryWait=.25,
            timeout=20))
