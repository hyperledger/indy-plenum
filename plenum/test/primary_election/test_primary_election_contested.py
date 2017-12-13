import pytest

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.messages.node_messages import Nomination
from plenum.test.delayers import delayerMsgTuple
from plenum.test.primary_election.helpers import checkNomination
from plenum.test.test_node import TestNodeSet, checkPoolReady, \
    checkProtocolInstanceSetup
from plenum.test import waits


nodeCount = 4

logger = getlogger()


@pytest.fixture()
def electContFixture(startedNodes: TestNodeSet):
    A, B, C, D = startedNodes.nodes.values()

    # Delaying nodeB' self nomination so nodeA's nomination can reach NodeC
    # and NodeD
    B.delaySelfNomination(2)

    # For B delay nominate messages from A and C
    for node in [A, C]:
        B.nodeIbStasher.delay(delayerMsgTuple(10, Nomination, node.name))

    for node in [C, D]:
        B.nodeIbStasher.delay(delayerMsgTuple(5, Nomination, node.name))
        node.delaySelfNomination(10)


# noinspection PyIncorrectDocstring
@pytest.mark.skip('Nodes use round robin primary selection')
def testPrimaryElectionContested(electContFixture, looper, keySharedNodes):
    """
    Primary selection (Rainy Day)
    A, B, C, D, E
    A, B, C, D startup. E is lagging.
    A sees the minimum number of nodes, and then sends Nominate(A)
    At the same exact time, B sees the minimum number of nodes, and then sends out Nominate(B)
    A sees B sending Nominate(B), but it has already nominated itself, so it does nothing
    B sees A sending Nominate(A), but it has already nominated itself, so it does nothing
    C sees A sending Nominate(A), and sends Nominate(A)
    D sees A sending Nominate(A), and sends Nominate(A)
    All nodes see that B nominated B and A, C, and D all nominated A
    Because the votes for A exceeds the votes for B, all send out Primary(A)
    TODO's (see below)
    All see the others have sent Primary A, and then the nodes record who is the Primary.
    """

    nodeSet = keySharedNodes
    A, B, C, D = nodeSet.nodes.values()

    checkPoolReady(looper, nodeSet)

    logger.debug("Check nomination")
    timeout = waits.expectedPoolNominationTimeout(nodeCount)

    # Checking whether Node A nominated itself
    looper.run(eventually(checkNomination, A, A.name,
                          retryWait=1, timeout=timeout))

    # Checking whether Node B nominated itself
    looper.run(eventually(checkNomination, B, B.name,
                          retryWait=1, timeout=timeout))

    for n in [C, D]:
        # Checking whether Node C and Node D nominated Node A
        looper.run(eventually(checkNomination, n, A.name,
                              retryWait=1, timeout=timeout))

    checkProtocolInstanceSetup(looper=looper, nodes=nodeSet, retryWait=1)

    # Node D should not be primary
    assert not D.hasPrimary
    # A should have at least one primary
    assert A.hasPrimary
