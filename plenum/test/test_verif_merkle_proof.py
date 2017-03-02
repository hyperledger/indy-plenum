from plenum.client.client import Client
from stp_core.loop.eventually import eventually
from plenum.test.helper import checkSufficientRepliesReceived, \
    sendRandomRequest
from plenum.test.test_client import TestClient


def testMerkleProofForFirstLeaf(client1: TestClient, replied1):
    replies = client1.getRepliesFromAllNodes(*replied1.key).values()
    assert Client.verifyMerkleProof(*replies)


def testMerkleProofForNonFirstLeaf(looper, nodeSet, wallet1, client1, replied1):
    req2 = sendRandomRequest(wallet1, client1)
    f = nodeSet.f
    looper.run(eventually(checkSufficientRepliesReceived, client1.inBox, req2.reqId
                          , f, retryWait=1, timeout=15))
    replies = client1.getRepliesFromAllNodes(*req2.key).values()
    assert Client.verifyMerkleProof(*replies)
