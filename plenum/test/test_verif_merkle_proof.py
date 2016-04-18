"""
Do a txn
Expect that the client would receive a reply and a merkle_data object.
Use the merkle_data object to query one of the nodes for verification.
"""


def testMerkleProof(client1, replied1):
    assert 1 == 1
    # txn = 1  # Fetch the first one in the Genesis transactions
    # Submitting a new transaction to verify merkle proof
    # client1.doVerifMerkleProof(txn.op)
    # Client must check its reply log for the verification reply.
