import pytest

from zeno.common.request_types import Primary
from zeno.test.eventually import eventually

whitelist = ['got primary declaration',
             'doing nothing for now',
             'know how to handle it']


# noinspection PyIncorrectDocstring
@pytest.mark.xfail(reason="blacklisting over primary declaration is disabled")
def testBlacklistNodeOnMultiplePrimaryDeclarations(looper,
                                                   keySharedNodes,
                                                   ready):
    """
    A node that sends multiple primary declarations must be blacklisted by
    other nodes
    """
    nodeSet = keySharedNodes
    A, B, C, D = nodeSet.nodes.values()

    # B sends more than one primary declaration
    B.send(Primary(D.name, 0, B.viewNo))
    B.send(Primary(D.name, 0, B.viewNo))

    # B should be blacklisted by A, C, D
    def chk():
        for node in A, C, D:
            assert node.isNodeBlacklisted(B.name)

    looper.run(eventually(chk, retryWait=1, timeout=3))
