import pytest

from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import Nomination
from plenum.test import waits

whitelist = ['already got nomination',
             'doing nothing for now']


# noinspection PyIncorrectDocstring,PyUnusedLocal,PyShadowingNames
@pytest.mark.skip(reason="SOV-540. Implementation changed.")
def testBlacklistNodeOnMultipleNominations(looper, keySharedNodes, ready):
    """
    A node that sends multiple nominations must be blacklisted by other nodes
    """
    nodeSet = keySharedNodes
    A, B, C, D = nodeSet.nodes.values()

    # B sends more than 2 nominations
    for i in range(3):
        B.send(Nomination(D.name, 0, B.viewNo))

    # B should be blacklisted by A, C, D
    def chk():
        for node in A, C, D:
            assert node.isNodeBlacklisted(B.name)

    timeout = waits.expectedPoolNominationTimeout(len(nodeSet.nodes))
    looper.run(eventually(chk, retryWait=1, timeout=timeout))
