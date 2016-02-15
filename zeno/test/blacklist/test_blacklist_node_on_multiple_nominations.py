import pytest

from zeno.common.request_types import Nomination
from zeno.test.eventually import eventually

whitelist = ['already got nomination',
             'doing nothing for now']


# noinspection PyIncorrectDocstring,PyUnusedLocal,PyShadowingNames
@pytest.mark.xfail(reason="blacklisting over nomination is disabled")
def testBlacklistNodeOnMultipleNominations(looper, keySharedNodes, ready):
    """
    A node that sends multiple nominations must be blacklisted by other nodes
    """
    nodeSet = keySharedNodes
    A, B, C, D = nodeSet.nodes.values()

    # B sends more than one nomination
    B.send(Nomination(D.name, 0, B.viewNo))
    B.send(Nomination(D.name, 0, B.viewNo))

    # B should be blacklisted by A, C, D
    def chk():
        for node in A, C, D:
            assert node.isNodeBlacklisted(B.name)

    looper.run(eventually(chk, retryWait=1, timeout=3))
