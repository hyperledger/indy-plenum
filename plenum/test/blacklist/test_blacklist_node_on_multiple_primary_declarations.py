import pytest

from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import Primary
from plenum.test import waits

whitelist = ['got primary declaration',
             'doing nothing for now',
             'know how to handle it']


# noinspection PyIncorrectDocstring
@pytest.mark.skip(reason="SOV-541. Implementation changed.")
def testBlacklistNodeOnMultiplePrimaryDeclarations(looper,
                                                   txnPoolNodeSet):
    """
    A node that sends multiple primary declarations must be blacklisted by
    other nodes
    """
    A, B, C, D = txnPoolNodeSet

    # B sends more than 2 primary declarations
    for i in range(3):
        B.send(Primary(D.name, 0, B.viewNo))

    # B should be blacklisted by A, C, D
    def chk():
        for node in A, C, D:
            assert node.isNodeBlacklisted(B.name)

    timeout = waits.expectedPoolNominationTimeout(len(txnPoolNodeSet))
    looper.run(eventually(chk, retryWait=1, timeout=timeout))
