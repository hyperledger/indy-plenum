import pytest

from plenum.client.client import Client
from plenum.test.helper import waitForSufficientRepliesForRequests, \
    sendRandomRequest
from plenum.test.test_client import TestClient


@pytest.skip('Wait for decision')
def testMerkleProofForFirstLeaf(client1: TestClient, replied1):
    replies = client1.getRepliesFromAllNodes(*replied1.key).values()
    assert Client.verifyMerkleProof(*replies)


@pytest.skip('Wait for decision')
def testMerkleProofForNonFirstLeaf(
        looper, txnPoolNodeSet, wallet1, client1, replied1):
    req2 = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[req2])
    replies = client1.getRepliesFromAllNodes(*req2.key).values()
    assert Client.verifyMerkleProof(*replies)
