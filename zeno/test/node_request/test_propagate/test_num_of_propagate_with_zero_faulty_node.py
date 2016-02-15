nodeCount = 7

# Default Nodes will be 0, due to faultyNodes Fixture
"""
    Tests for a particular client id and request id, how many propagate requests seen by a particular node

    A gets REQ
    B gets REQ
    C gets REQ
    D gets REQ

    A sends PROP to B, C, D
    B sends PROP to A, C, D
    C sends PROP to A, B, D
    D sends PROP to A, B, C

    B gets PROP from A, and it already has that message and has already propagated to others, so it doesn't propagate
        however, so it can now forward to replicas, and sends PROP to C and D

    ASSUMPTION: Once a node sends a PROP for a request, it never sends a PROP for that request again.
"""


def testNumOfPropagateWithZeroFaultyNode(propagated1):
    pass
