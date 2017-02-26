import pytest
from raet.raeting import AutoMode

from plenum.common.exceptions import EmptySignature, BlowUp, NotConnectedToAny
from plenum.common.exceptions import NotConnectedToAny
from plenum.test.helper import *
from plenum.test.helper import checkResponseCorrectnessFromNodes
from plenum.test.helper import randomOperation, \
    checkLastClientReqForNode, \
    getRepliesFromClientInbox
from plenum.test.helper import sendRandomRequest, checkSufficientRepliesRecvd, \
    assertLength
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_client import genTestClient
from plenum.test.test_node import TestNodeSet
from plenum.common.crypto import Signer

nodeCount = 7

F = getMaxFailures(nodeCount)

whitelist = ['signer not configured so not signing',
             'for EmptySignature',
             'discarding message',
             'found legacy entry',
             'public key from disk',
             'verification key from disk']  # warnings


def checkResponseRecvdFromNodes(client, expectedCount: int,
                                expectedReqId: int):
    # Checks if the client has unique `expectedCount` number of REQACKs and
    # REPLYs from nodes. It ignores more than one REQACK or REPLY since a
    # client might be retrying
    acks = set()
    replies = set()
    for (resp, nodeNm) in client.inBox:
        op = resp.get(OP_FIELD_NAME)
        if op == REPLY:
            reqId = resp.get(f.RESULT.nm, {}).get(f.REQ_ID.nm)
            coll = replies
        elif op == REQACK:
            reqId = resp.get(f.REQ_ID.nm)
            coll = acks
        else:
            continue
        if reqId == expectedReqId:
            coll.add(nodeNm)
    assert len(replies) == len(acks) == expectedCount


# noinspection PyIncorrectDocstring
@pytest.mark.skip(reason="SOV-550. Implementation changed")
def testGeneratedRequestSequencing(tdir_for_func):
    """
    Request ids must be generated in an increasing order
    """
    with TestNodeSet(count=4, tmpdir=tdir_for_func) as nodeSet:
        w = Wallet("test")
        w.addIdentifier()

        operation = randomOperation()

        request = w.signOp(operation)
        assert request.reqId == 1

        request = w.signOp(operation)
        assert request.reqId == 2

        request = w.signOp(randomOperation())
        assert request.reqId == 3

        idr, _ = w.addIdentifier()

        request = w.signOp(randomOperation(), idr)
        assert request.reqId == 1

        request = w.signOp(randomOperation())
        assert request.reqId == 4


# noinspection PyIncorrectDocstring
def testClientShouldNotBeAbleToConnectToNodesNodeStack(pool):
    """
    Client should not be able to connect to nodes in the node's nodestack
    """

    async def go(ctx):
        for n in ctx.nodeset:
            n.nodestack.keep.auto = AutoMode.never

        nodestacksVersion = {k: v.ha for k, v in ctx.nodeset.nodeReg.items()}
        client1, _ = genTestClient(nodeReg=nodestacksVersion, tmpdir=ctx.tmpdir)

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
        request = client1.submitReqs(request)[0]
        with pytest.raises(AssertionError):
            for node in ctx.nodeset:
                await eventually(
                        checkLastClientReqForNode, node, request,
                        retryWait=1, timeout=10)

        for n in ctx.nodeset:
            params = n.spylog.getLastParams(Node.handleInvalidClientMsg)
            ex = params['ex']
            _, frm = params['wrappedMsg']
            assert isinstance(ex, EmptySignature)
            assert frm == client1.stackName

            params = n.spylog.getLastParams(Node.discard)
            reason = params["reason"]
            (msg, frm) = params["msg"]
            assert msg == request.__dict__
            assert frm == client1.stackName
            assert "EmptySignature" in reason

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
def testReplyWhenRepliesFromAllNodesAreSame(looper, client1, wallet1):
    """
    When there are not faulty nodes, the client must get a reply from all the
    nodes.
    """
    request = sendRandomRequest(wallet1, client1)
    looper.run(
            eventually(checkResponseRecvdFromNodes, client1,
                       nodeCount, request.reqId,
                       retryWait=1, timeout=20))
    checkResponseCorrectnessFromNodes(client1.inBox, request.reqId, F)


# noinspection PyIncorrectDocstring
def testReplyWhenRepliesFromExactlyFPlusOneNodesAreSame(looper,
                                                        client1,
                                                        wallet1):
    """
    When only :math:`2f+1` replies from the nodes are matching, the client
    would accept the reply
    """
    request = sendRandomRequest(wallet1, client1)
    # exactly f + 1 => (3) nodes have correct responses
    # modify some (numOfResponses of type REPLY - (f + 1)) => 4 responses to
    # have a different operations
    looper.run(
            eventually(checkResponseRecvdFromNodes, client1,
                       nodeCount, request.reqId,
                       retryWait=1, timeout=20))

    replies = (msg for msg, frm in client1.inBox
               if msg[OP_FIELD_NAME] == REPLY and
               msg[f.RESULT.nm][f.REQ_ID.nm] == request.reqId)

    # change two responses to something different
    for i in range(2):
        msg = next(replies)
        msg[f.RESULT.nm][TXN_ID] = str(i) + "Some random id"

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
                          retryWait=.5,
                          timeout=5))
    originalRequestResponsesLen = nodeCount * 2
    duplicateRequestRepliesLen = nodeCount  # for a duplicate request we need to
    client1.nodestack._enqueueIntoAllRemotes(sent1, None)

    def chk():
        assertLength([response for response in client1.inBox
                      if (response[0].get(f.RESULT.nm) and
                       response[0][f.RESULT.nm][f.REQ_ID.nm] == sent1.reqId) or
                      (response[0].get(OP_FIELD_NAME) == REQACK and
                       response[0].get(f.REQ_ID.nm) == sent1.reqId)],
                     originalRequestResponsesLen + duplicateRequestRepliesLen)

    looper.run(eventually(
            chk,
            retryWait=1,
            timeout=20))


# noinspection PyIncorrectDocstring
def testReplyMatchesRequest(looper, nodeSet, tdir, up):
    '''
    This tests does check following things:
      - wallet works correctly when used by multiple clients
      - clients do receive responses for exactly the same request they sent
    '''

    def makeClient(id):
        client, wallet = genTestClient(nodeSet,
                                       tmpdir=tdir,
                                       name="client-{}".format(id))
        looper.add(client)
        looper.run(client.ensureConnectedToNodes())
        return client, wallet

    # creating clients
    numOfClients = 3
    numOfRequests = 1

    clients = set()
    sharedWallet = None
    for i in range(numOfClients):
        client, wallet = makeClient(i)
        if sharedWallet is None:
            sharedWallet = wallet
        clients.add(client)

    for i in range(1, numOfRequests + 1):

        # sending requests
        requests = {}
        for client in clients:
            op = randomOperation()
            req = sharedWallet.signOp(op)

            request = client.submitReqs(req)[0]
            requests[client] = (request.reqId, request.operation['amount'])

        # checking results
        for client, (reqId, sentAmount) in requests.items():
            looper.run(eventually(checkResponseRecvdFromNodes,
                                  client,
                                  nodeCount,
                                  reqId,
                                  retryWait=1,
                                  timeout=25))

            print("Expected amount for request {} is {}".
                  format(reqId, sentAmount))

            replies = [r[0]['result']['amount']
                       for r in client.inBox
                       if r[0]['op'] == 'REPLY'
                       and r[0]['result']['reqId'] == reqId]

            assert all(replies[0] == r for r in replies)
            assert replies[0] == sentAmount


def testReplyReceivedOnlyByClientWhoSentRequest(looper, nodeSet, tdir,
                                                client1, wallet1):
    newClient, _ = genTestClient(nodeSet, tmpdir=tdir)
    looper.add(newClient)
    looper.run(newClient.ensureConnectedToNodes())
    client1InboxSize = len(client1.inBox)
    newClientInboxSize = len(newClient.inBox)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, newClient, 1)
    assert len(client1.inBox) == client1InboxSize
    assert len(newClient.inBox) > newClientInboxSize


def testClientCanSendMessagesIfAnotherClientSendsMessage(looper, nodeSet,
                                                         tdir, another_tdir,
                                                         wallet1):
    assert tdir != another_tdir
    client1 = createClientSendMessageAndRemove(looper, nodeSet,
                                               tdir, wallet1, 'TestClient1')
    client2 = createClientSendMessageAndRemove(looper, nodeSet,
                                               another_tdir, wallet1,
                                               'TestClient1')
    clientSendMessageAndRemove(client1, looper, wallet1)


def testClientCanSendMessagesIfInitWithSighex(looper, nodeSet,
                                                         tdir, another_tdir,
                                                         wallet1):
    assert tdir != another_tdir
    signer1 = Signer()
    sighex1 = signer1.keyhex
    client1 = createClientSendMessageAndRemove(looper, nodeSet,
                                               tdir, wallet1,
                                               'TestClient1', sighex=sighex1)

    signer2 = Signer()
    sighex2 = signer2.keyhex
    assert sighex2 != sighex1
    client2 = createClientSendMessageAndRemove(looper, nodeSet,
                                               another_tdir, wallet1,
                                               'TestClient1', sighex=sighex2)
    clientSendMessageAndRemove(client1, looper, wallet1)