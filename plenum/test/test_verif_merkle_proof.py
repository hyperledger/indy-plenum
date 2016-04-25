from plenum.client.client import Client
from plenum.common.util import getMaxFailures
from plenum.test.eventually import eventually
from plenum.test.helper import randomOperation, checkSufficientRepliesRecvd, \
    sendRandomRequest


def testMerkleProofForFirstLeaf(client1: Client, replied1):
    replies = client1.getRepliesFromAllNodes(1).values()
    assert Client.verifyMerkleProof(*replies)


def testMerkleProofForNonFirstLeaf(looper, nodeSet, client1: Client, replied1):
    req2 = sendRandomRequest(client1)
    f = nodeSet.f
    looper.run(eventually(checkSufficientRepliesRecvd, client1.inBox, req2.reqId, f, retryWait=1, timeout=15))
    replies = client1.getRepliesFromAllNodes(req2.reqId).values()
    assert Client.verifyMerkleProof(*replies)
