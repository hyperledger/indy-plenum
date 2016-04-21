"""
Do a txn
Expect that the client would receive a reply and a merkle_data object.
Use the merkle_data object to query one of the nodes for verification.
"""
from ledger.immutable_store.store import F
from plenum.client.client import Client
from plenum.common.util import getMaxFailures
from plenum.test.eventually import eventually
from plenum.test.helper import checkSufficientRepliesRecvd


# def testMerkleProof(looper, nodeSet, client1: Client, replied1):
#     reply = client1.getReply(1)[0]
#     keys = [F.serialNo.name, F.STH.name, F.auditInfo.name]
#     merkleInfo = {k: reply(k) for k in keys}
#     identifier, _ = client1.signers.popitem()
#     client1.doVerifMerkleProof(merkleInfo, identifier)
#     # nodeSet.Alpha.verifyMerkleProof(2,  reply, identifier)
#     f = getMaxFailures(len(nodeSet))
#     looper.run(eventually(checkSufficientRepliesRecvd, client1.inBox, 2, f))
#     # txn = 1  # Fetch the first one in the Genesis transactions
#     # Submitting a new transaction to verify merkle proof
#     # client1.doVerifMerkleProof(txn.op)
#     # Client must check its reply log for the verification reply.

def testMerkleProof(client1: Client, replied1):
    replies = client1.getRepliesFromAllNodes(1).values()
    assert Client.verifyMerkleProof(*replies)
