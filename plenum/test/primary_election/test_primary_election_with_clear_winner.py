import pytest

from stp_core.loop.eventually import eventually
from plenum.test.primary_election.helpers import checkNomination
from plenum.test.test_node import checkPoolReady, \
    checkProtocolInstanceSetup
from plenum.test import waits

nodeCount = 4


@pytest.fixture()
def electContFixture(txnPoolNodeSet):
    A, B, C, D = txnPoolNodeSet

    for node in [B, C, D]:
        node.delaySelfNomination(4)


# noinspection PyIncorrectDocstring
@pytest.mark.skip('Nodes use round robin primary selection')
def testPrimaryElectionWithAClearWinner(
        electContFixture, looper, txnPoolNodeSet):
    """
    Primary selection (Sunny Day)
    A, B, C, D, E
    A, B, C, D startup. E is lagging.
    A sees the minimum number of nodes first, and then sends out a NOMINATE(A) message
    B, C, D all see the NOMINATE(A) message from A, and respond with NOMINATE(A) message to all other nodes

    A sees three other NOMINATE(A) votes (from B, C, D)
    A sees that A is the clear winner (2f+1 total), and sends PRIMARY(A) to all nodes

    B sees two more NOMINATE(A) votes (from C and D)
    B sees that A is the clear winner (2f+1 total), and sends PRIMARY(A) to all nodes

    C sees two more NOMINATE(A) votes (from B and D)
    C sees that A is the clear winner (2f+1 total), and sends PRIMARY(A) to all nodes

    D sees two more NOMINATE(A) votes (from B and C)
    D sees that A is the clear winner (2f+1 total), and sends PRIMARY(A) to all nodes

    A sees at least two other PRIMARY(A) votes (3 including it's own)
    selects A as primary

    B sees at least two other PRIMARY(A) votes (3 including it's own)
    selects A as primary

    C sees at least two other PRIMARY(A) votes (3 including it's own)
    selects A as primary

    D sees at least two other PRIMARY(A) votes (3 including it's own)
    selects A as primary
    """
    A, B, C, D = txnPoolNodeSet
    nodesBCD = [B, C, D]

    checkPoolReady(looper, txnPoolNodeSet)

    # Checking whether one of the replicas of Node A nominated itself
    timeout = waits.expectedPoolNominationTimeout(len(txnPoolNodeSet))
    looper.run(eventually(checkNomination, A,
                          A.name, retryWait=1, timeout=timeout))

    timeout = waits.expectedPoolNominationTimeout(len(txnPoolNodeSet))
    for n in nodesBCD:
        # Checking whether Node B, C and D nominated Node A
        looper.run(eventually(checkNomination, n, A.name,
                              retryWait=1, timeout=timeout))

    checkProtocolInstanceSetup(looper=looper, nodes=txnPoolNodeSet, retryWait=1)
    assert A.hasPrimary
