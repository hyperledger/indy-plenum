from plenum.client.client import Client
from plenum.common.util import getMaxFailures
from plenum.test.eventually import eventually
from plenum.test.helper import randomOperation, checkSufficientRepliesRecvd


def testMerkleProofForFirstLeaf(client1: Client, replied1):
    replies = client1.getRepliesFromAllNodes(1).values()
    assert Client.verifyMerkleProof(*replies)


def testMerkleProofForNonFirstLeaf(looper, nodeSet, client1: Client, replied1):
    client1.submit(randomOperation())
    f = getMaxFailures(len(nodeSet))
    looper.run(eventually(checkSufficientRepliesRecvd, client1.inBox, 2, f))
    replies = client1.getRepliesFromAllNodes(2).values()
    assert Client.verifyMerkleProof(*replies)
