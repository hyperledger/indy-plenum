import pytest

from plenum.common.types import Primary
from plenum.test.eventually import eventually

whitelist = ['got primary declaration',
             'doing nothing for now',
             'know how to handle it']


# noinspection PyIncorrectDocstring
@pytest.mark.skipif(True, reason="Implementation changed.")
def testBlacklistNodeOnMultiplePrimaryDeclarations(looper,
                                                   keySharedNodes,
                                                   ready):
    """
    A node that sends multiple primary declarations must be blacklisted by
    other nodes
    """
    nodeSet = keySharedNodes
    A, B, C, D = nodeSet.nodes.values()

    # B sends more than 2 primary declarations
    for i in range(3):
        B.send(Primary(D.name, 0, B.viewNo))

    # B should be blacklisted by A, C, D
    def chk():
        for node in A, C, D:
            assert node.isNodeBlacklisted(B.name)

    looper.run(eventually(chk, retryWait=1, timeout=3))
